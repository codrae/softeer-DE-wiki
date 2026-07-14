#!/usr/bin/env python3
"""
설정 변경 스크립트
사용법: python3 modify_config.py <config_dir>

config_dir 안에는 core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml
4개 파일이 있어야 함 (b-2 결정: 이미 목표값이 채워진 완성본 상태로 시작).
"""
import os
import sys
import shutil
import subprocess
from datetime import datetime
from xml.etree import ElementTree as ET

# 우리가 결정한 것들을 여기 상수로 정리
TARGET_VALUES = {
    "core-site.xml": {
        "fs.defaultFS": "hdfs://namenode:9000",
        "hadoop.tmp.dir": "/hadoop/tmp",
        "io.file.buffer.size": "131072",
    },
    "hdfs-site.xml": {
        "dfs.replication": "2",
        "dfs.blocksize": "134217728",
        "dfs.namenode.name.dir": "/hadoop/dfs/name",
        "dfs.datanode.data.dir": "/hadoop/dfs/data",              # 추가: volume 마운트 경로와 일치 필수
    },
    "mapred-site.xml": {
        "mapreduce.framework.name": "yarn",
        "mapreduce.jobhistory.address": "namenode:10020",
        "mapreduce.task.io.sort.mb": "256",
        "mapreduce.job.tracker": "namenode:9001",
        "mapreduce.map.java.opts": "-Xmx768m",                     # 추가: JVM 기본 힙 부족 방지
        "mapreduce.reduce.java.opts": "-Xmx768m",                  # 추가
    },
    "yarn-site.xml": {
        "yarn.resourcemanager.address": "namenode:8032",
        "yarn.nodemanager.resource.memory-mb": "2048",             # 수정: 8192 -> 2048 (로컬 환경 제약)
        "yarn.scheduler.minimum-allocation-mb": "1024",
        "yarn.resourcemanager.hostname": "namenode",               # 추가: resource-tracker 주소 유도 필수
        "yarn.nodemanager.aux-services": "mapreduce_shuffle",      # 추가: reduce shuffle 필수
    },
}

# mapreduce.job.tracker는 파일에 넣을 때만 참고용 description 부여 (신규 생성 시에만 사용됨)
PROPERTY_DESCRIPTIONS = {
    "mapreduce.job.tracker": (
        "Hadoop 1.x(JobTracker) 시절 설정. YARN 프레임워크에서는 "
        "ResourceManager+ApplicationMaster로 대체되어 이 값은 무시됨. "
        "학습 목적으로 검증 대상에는 포함."
    ),
}

# C 결정: 4개 컨테이너 모두 4개 파일 전부 배포 (부분집합 아님)
CONTAINERS = ["namenode", "worker1", "worker2", "worker3"]
REMOTE_CONF_DIR = "/opt/hadoop/etc/hadoop"  # Dockerfile의 COPY 대상 경로와 일치시킴
BACKUP_DIR = "backups"

# C 결정: 컨테이너 역할별로 재시작해야 할 데몬 목록
# (daemon 스크립트, daemon 이름) 튜플의 리스트
DAEMON_MAP = {
    "namenode": [
        ("hadoop-daemon.sh", "namenode"),
        ("yarn-daemon.sh", "resourcemanager"),
        ("mr-jobhistory-daemon.sh", "historyserver"),
    ],
    "worker": [
        ("hadoop-daemon.sh", "datanode"),
        ("yarn-daemon.sh", "nodemanager"),
    ],
}


def backup_file(filepath):
    """
    B 결정: backups/ 디렉토리에 타임스탬프 파일명으로 저장.
    별도의 'original' 개념 없이 계속 쌓기만 함 —
    가장 이른 타임스탬프가 곧 원본이라는 원칙(B 최종 결정).
    """
    if not os.path.exists(filepath):
        return None

    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.basename(filepath)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"{filename}.{timestamp}")

    shutil.copy2(filepath, backup_path)
    return backup_path


def set_property(tree, name, value, description=None):
    """
    이미 있으면 <value>만 교체, 없으면 <property> 신규 생성.
    (A 결정: ElementTree 구조 인식 활용, description은 신규 생성시에만)
    """
    root = tree.getroot()
    elem = root.find(f".//property[name='{name}']")
    if elem is not None:
        elem.find("value").text = value
        return "updated"
    else:
        new_prop = ET.SubElement(root, "property")
        ET.SubElement(new_prop, "name").text = name
        ET.SubElement(new_prop, "value").text = value
        if description:
            ET.SubElement(new_prop, "description").text = description
        return "created"


def modify_config_file(filepath, target_values):
    """
    A 결정: ElementTree로 파싱 → 수정 → 저장.
    에러 종류별로 구분해서 상태를 리포트한다.
    """
    result = {"file": filepath, "status": "PASS", "changes": [], "error": None}

    try:
        tree = ET.parse(filepath)
    except FileNotFoundError:
        result["status"] = "FAIL"
        result["error"] = f"파일을 찾을 수 없음: {filepath}"
        return result
    except ET.ParseError as e:
        result["status"] = "FAIL"
        result["error"] = f"XML 파싱 실패 ({filepath}): {e}"
        return result

    try:
        for name, value in target_values.items():
            description = PROPERTY_DESCRIPTIONS.get(name)
            action = set_property(tree, name, value, description=description)
            result["changes"].append((name, value, action))

        tree.write(filepath, encoding="UTF-8", xml_declaration=True)
    except Exception as e:
        result["status"] = "FAIL"
        result["error"] = f"수정 중 오류 ({filepath}): {e}"

    return result


def deploy_to_containers(local_config_dir):
    """
    C 결정: docker cp 이중 루프 (컨테이너 수 x 파일 수).
    클러스터 전체가 동일 설정을 봐야 하므로 부분집합 배포는 하지 않는다.
    """
    results = []
    for container in CONTAINERS:
        for filename in TARGET_VALUES:
            local_path = os.path.join(local_config_dir, filename)
            remote_path = f"{container}:{REMOTE_CONF_DIR}/{filename}"
            try:
                subprocess.run(
                    ["docker", "cp", local_path, remote_path],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                results.append({"container": container, "file": filename, "status": "PASS"})
            except subprocess.CalledProcessError as e:
                results.append({
                    "container": container,
                    "file": filename,
                    "status": "FAIL",
                    "error": e.stderr.strip(),
                })
    return results


def restart_daemons():
    """
    C 결정: docker exec으로 각 컨테이너의 데몬 프로세스만 stop/start.
    (컨테이너 자체를 재시작하지 않음 -> entrypoint의 최초 포맷 체크 로직 재실행 방지)

    전체를 stop 한 뒤 전체를 start 하는 2단계 구조로 진행한다.
    - stop 순서는 무관 (어차피 클러스터 전체가 잠깐 내려가는 것이므로)
    - start는 namenode(마스터 데몬)가 먼저 떠야, datanode/nodemanager가
      등록할 대상이 준비된 상태에서 기동되므로 순서를 지킨다
    """
    results = []

    def _run(container, daemon_script, daemon_name, action):
        try:
            subprocess.run(
                ["docker", "exec", container, daemon_script, action, daemon_name],
                check=True,
                capture_output=True,
                text=True,
            )
            results.append({
                "container": container,
                "daemon": daemon_name,
                "action": action,
                "status": "PASS",
            })
        except subprocess.CalledProcessError as e:
            results.append({
                "container": container,
                "daemon": daemon_name,
                "action": action,
                "status": "FAIL",
                "error": e.stderr.strip(),
            })

    # 1단계: 전체 stop (순서 무관)
    for container in CONTAINERS:
        role = "namenode" if container == "namenode" else "worker"
        for daemon_script, daemon_name in DAEMON_MAP[role]:
            _run(container, daemon_script, daemon_name, "stop")

    # 2단계: 전체 start (namenode 먼저, worker는 그 다음)
    for daemon_script, daemon_name in DAEMON_MAP["namenode"]:
        _run("namenode", daemon_script, daemon_name, "start")

    for container in CONTAINERS:
        if container == "namenode":
            continue
        for daemon_script, daemon_name in DAEMON_MAP["worker"]:
            _run(container, daemon_script, daemon_name, "start")

    return results


def print_report(title, items, formatter):
    print(f"\n=== {title} ===")
    for item in items:
        print(formatter(item))


def main():
    if len(sys.argv) != 2:
        print("사용법: python3 modify_config.py <config_dir>")
        sys.exit(1)

    config_dir = sys.argv[1]

    # 1) 백업
    backup_results = []
    for filename in TARGET_VALUES:
        filepath = os.path.join(config_dir, filename)
        backup_path = backup_file(filepath)
        backup_results.append({"file": filename, "backup_path": backup_path})

    print_report(
        "1. 백업",
        backup_results,
        lambda r: f"  {r['file']} -> {r['backup_path'] or 'SKIP (원본 없음)'}"
    )

    # 2) 수정
    modify_results = []
    for filename, target_values in TARGET_VALUES.items():
        filepath = os.path.join(config_dir, filename)
        result = modify_config_file(filepath, target_values)
        modify_results.append(result)

    def _fmt_modify(r):
        if r["status"] == "FAIL":
            return f"  FAIL: {r['file']} -> {r['error']}"
        changes = ", ".join(f"{n}={v}({a})" for n, v, a in r["changes"])
        return f"  PASS: {r['file']} -> {changes}"

    print_report("2. 설정 수정", modify_results, _fmt_modify)

    if any(r["status"] == "FAIL" for r in modify_results):
        print("\n설정 수정 단계에서 실패가 발생하여 배포/재시작을 중단합니다.")
        sys.exit(1)

    # 3) 배포
    deploy_results = deploy_to_containers(config_dir)
    print_report(
        "3. 컨테이너 배포",
        deploy_results,
        lambda r: (
            f"  PASS: {r['container']} <- {r['file']}"
            if r["status"] == "PASS"
            else f"  FAIL: {r['container']} <- {r['file']} ({r.get('error')})"
        )
    )

    # 4) 재시작 (전체 stop -> 전체 start)
    restart_results = restart_daemons()
    print_report(
        "4. 데몬 재시작",
        restart_results,
        lambda r: (
            f"  PASS: {r['container']} {r['daemon']} {r['action']}"
            if r["status"] == "PASS"
            else f"  FAIL: {r['container']} {r['daemon']} {r['action']} ({r.get('error')})"
        )
    )

    print("\n설정 변경 스크립트 실행 완료.")


if __name__ == "__main__":
    main()