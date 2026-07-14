#!/usr/bin/env python3
"""
검증 스크립트
사용법: python3 verify_config.py

modify_config.py 실행(백업->수정->배포->재시작) 이후에 실행하는 것을 전제로 한다.
"""
import subprocess
import time
import requests

# modify_config.py의 TARGET_VALUES와 동일한 기대값 (여기서는 검증 대상만 별도 상수로 관리)
EXPECTED = {
    "core-site.xml": {
        "fs.defaultFS": "hdfs://namenode:9000",
        "hadoop.tmp.dir": "/hadoop/tmp",
        "io.file.buffer.size": "131072",
    },
    "hdfs-site.xml": {
        "dfs.replication": "2",
        "dfs.blocksize": "134217728",
        "dfs.namenode.name.dir": "/hadoop/dfs/name",
    },
    "mapred-site.xml": {
        "mapreduce.framework.name": "yarn",
        "mapreduce.jobhistory.address": "namenode:10020",
        "mapreduce.task.io.sort.mb": "256",
    },
    "yarn-site.xml": {
        "yarn.resourcemanager.address": "namenode:8032",
        "yarn.nodemanager.resource.memory-mb": "8192",
        "yarn.scheduler.minimum-allocation-mb": "1024",
    },
}

# Q1 결정: job.tracker는 "설정되지만 YARN에서 무시됨"으로 별도 리포트
JOB_TRACKER_EXPECTED = "namenode:9001"

EXPECTED_DATANODES = 3
EXPECTED_NODEMANAGERS = 3

NAMENODE_HOST = "localhost"  # docker-compose.yml에서 호스트로 포트 매핑된 이름 기준
NAMENODE_WEB_PORT = 50070
RM_WEB_PORT = 8088


def run_in_container(container, *cmd):
    """docker exec 결과의 stdout을 문자열로 반환. 실패 시 None."""
    try:
        result = subprocess.run(
            ["docker", "exec", container, *cmd],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return None


def check_safemode_off():
    """
    NameNode가 safe mode에서 벗어났는지 확인.
    재시작 직후 datanode 블록 리포트가 threshold(기본 0.999)에 도달하기 전까지는
    쓰기 작업(mkdir, put 등)이 전부 막히므로, readiness check에 반드시 포함해야 한다.
    """
    output = run_in_container("namenode", "hdfs", "dfsadmin", "-safemode", "get")
    if output is None:
        return False
    return "Safe mode is OFF" in output


def wait_for_readiness(timeout=90, interval=5):
    """
    D-4 결정: 모든 검증에 앞서 datanode/nodemanager가 기대 개수만큼
    등록되고, NameNode가 safe mode에서 벗어날 때까지 폴링.
    이후 개별 검증 단계는 재시도 로직 없이 단순화 가능.
    """
    print("=== 0. Readiness Check ===")
    elapsed = 0
    while elapsed <= timeout:
        try:
            hdfs_report = requests.get(
                f"http://{NAMENODE_HOST}:{NAMENODE_WEB_PORT}/jmx",
                params={"qry": "Hadoop:service=NameNode,name=FSNamesystem"},
                timeout=5,
            ).json()
            live_datanodes = hdfs_report["beans"][0].get("NumLiveDataNodes", 0)
        except Exception:
            live_datanodes = 0

        try:
            rm_metrics = requests.get(
                f"http://{NAMENODE_HOST}:{RM_WEB_PORT}/ws/v1/cluster/metrics", timeout=5
            ).json()
            active_nodemanagers = rm_metrics["clusterMetrics"]["activeNodes"]
        except Exception:
            active_nodemanagers = 0

        safemode_off = check_safemode_off()

        print(f"  [{elapsed}s] datanode={live_datanodes}/{EXPECTED_DATANODES}, "
              f"nodemanager={active_nodemanagers}/{EXPECTED_NODEMANAGERS}, "
              f"safemode_off={safemode_off}")

        if (live_datanodes >= EXPECTED_DATANODES
                and active_nodemanagers >= EXPECTED_NODEMANAGERS
                and safemode_off):
            print("  PASS: 모든 노드 등록 완료 + safe mode 해제 확인")
            return True

        time.sleep(interval)
        elapsed += interval

    print("  FAIL: timeout 내 준비 완료 상태에 도달하지 못함 (아래 검증은 이 상태 기준으로 진행)")
    return False


def check_config_values():
    """
    D-1 결정: getconf(파일 기준) + 웹 /conf 엔드포인트(데몬 실제 로드값) 이중 확인
    """
    print("\n=== 1. 설정값 검증 ===")
    results = []

    for filename, props in EXPECTED.items():
        for key, expected_value in props.items():
            actual = run_in_container("namenode", "hdfs", "getconf", "-confKey", key)
            status = "PASS" if actual == expected_value else "FAIL"
            print(f"  [{status}] (getconf) {key} -> {actual} (expected {expected_value})")
            results.append((key, "getconf", status))

    actual_jt = run_in_container("namenode", "hdfs", "getconf", "-confKey", "mapreduce.job.tracker")
    jt_status = "PASS" if actual_jt == JOB_TRACKER_EXPECTED else "FAIL"
    print(f"  [{jt_status}] (getconf) mapreduce.job.tracker -> {actual_jt} "
          f"(설정은 반영되지만 YARN 모드에서는 무시됨)")
    results.append(("mapreduce.job.tracker", "getconf", jt_status))

    try:
        nn_conf = requests.get(f"http://{NAMENODE_HOST}:{NAMENODE_WEB_PORT}/conf", timeout=5).text
        rm_conf = requests.get(f"http://{NAMENODE_HOST}:{RM_WEB_PORT}/conf", timeout=5).text
        for filename, props in EXPECTED.items():
            for key, expected_value in props.items():
                found = expected_value in nn_conf or expected_value in rm_conf
                status = "PASS" if found else "FAIL"
                print(f"  [{status}] (daemon /conf) {key}={expected_value}")
                results.append((key, "daemon_conf", status))
    except Exception as e:
        print(f"  FAIL: /conf 엔드포인트 조회 실패 ({e})")

    return results


def check_replication_factor(retries=3, interval=5):
    """
    E 결정: 설정값 / 실제값(신규 파일) / 연결된 datanode 수 3가지 분리 리포트.
    재시작 직후 하트비트 재등록 지연을 감안해 retry 포함.
    """
    print("\n=== 2. Replication Factor 검증 ===")

    config_value = run_in_container("namenode", "hdfs", "getconf", "-confKey", "dfs.replication")
    print(f"  [설정값] dfs.replication -> {config_value}")

    try:
        report = requests.get(
            f"http://{NAMENODE_HOST}:{NAMENODE_WEB_PORT}/jmx",
            params={"qry": "Hadoop:service=NameNode,name=FSNamesystem"},
            timeout=5,
        ).json()
        datanode_count = report["beans"][0].get("NumLiveDataNodes", 0)
    except Exception:
        datanode_count = "조회 실패"
    print(f"  [클러스터 상태] 연결된 datanode 수 -> {datanode_count}")

    test_file = "/verify_test_file.txt"
    run_in_container("namenode", "bash", "-c", f"echo 'w3m2b test' | hdfs dfs -put -f - {test_file}")

    actual_repl = None
    for attempt in range(1, retries + 1):
        actual_repl = run_in_container("namenode", "hdfs", "dfs", "-stat", "%r", test_file)
        if actual_repl == config_value:
            break
        print(f"  [재시도 {attempt}/{retries}] 실제 replication={actual_repl}, "
              f"{interval}초 후 재확인...")
        time.sleep(interval)

    status = "PASS" if actual_repl == config_value else "FAIL"
    print(f"  [{status}] [실제 데이터] 테스트 파일 replication -> {actual_repl} (설정값 {config_value})")

    return status == "PASS"


def check_yarn_job():
    """
    D-2 결정: MR job 실행 후 application ID 발급 여부 + 최종 상태 확인
    """
    print("\n=== 3. MapReduce/YARN Job 검증 ===")

    output = run_in_container(
        "namenode", "bash", "-c",
        "hadoop jar "
        "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar "
        "pi 2 10 2>&1"
    )

    if output is None:
        print("  FAIL: job 실행 자체가 실패함")
        return False

    app_id = None
    for line in output.splitlines():
        if "application_" in line:
            for token in line.split():
                if token.startswith("application_"):
                    app_id = token.strip(":,")
                    break
        if app_id:
            break

    if not app_id:
        print("  FAIL: application_ID가 발급되지 않음 (YARN을 거치지 않았을 가능성)")
        return False

    print(f"  PASS: application ID 발급됨 -> {app_id}")

    status_output = run_in_container("namenode", "yarn", "application", "-status", app_id)
    is_finished = status_output and "FINISHED" in status_output and "SUCCEEDED" in status_output
    status = "PASS" if is_finished else "FAIL"
    print(f"  [{status}] job 최종 상태 확인 (FINISHED/SUCCEEDED 여부)")

    return is_finished


def check_yarn_memory():
    """
    D-3 결정: ResourceManager REST API로 클러스터 전체 가용 메모리 조회
    """
    print("\n=== 4. YARN 클러스터 메모리 검증 ===")
    try:
        metrics = requests.get(
            f"http://{NAMENODE_HOST}:{RM_WEB_PORT}/ws/v1/cluster/metrics", timeout=5
        ).json()
        total_mb = metrics["clusterMetrics"]["totalMB"]
        print(f"  [참고] 클러스터 전체 가용 메모리 -> {total_mb} MB")
        return total_mb
    except Exception as e:
        print(f"  FAIL: REST API 조회 실패 ({e})")
        return None


def main():
    ready = wait_for_readiness()
    check_config_values()
    check_replication_factor()
    check_yarn_job()
    check_yarn_memory()

    print("\n검증 스크립트 실행 완료.")
    if not ready:
        print("주의: readiness check가 timeout되어 일부 검증 결과가 정확하지 않을 수 있습니다.")


if __name__ == "__main__":
    main()