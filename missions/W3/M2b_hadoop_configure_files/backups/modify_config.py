#!/usr/bin/env python3
"""
설정 변경 스크립트
사용법: python3 modify_config.py <config_dir>
"""
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
    },
    "mapred-site.xml": {
        "mapreduce.framework.name": "yarn",
        "mapreduce.jobhistory.address": "namenode:10020",
        "mapreduce.task.io.sort.mb": "256",
        # Q1 결정: job.tracker도 같이 넣되, 검증 리포트에서 "무시됨"으로 표시
        "mapreduce.job.tracker": "namenode:9001",
    },
    "yarn-site.xml": {
        "yarn.resourcemanager.address": "namenode:8032",
        "yarn.nodemanager.resource.memory-mb": "8192",
        "yarn.scheduler.minimum-allocation-mb": "1024",
    },
}

# C 결정: 4개 컨테이너 모두 4개 파일 전부 배포 (부분집합 아님)
CONTAINERS = ["namenode", "worker1", "worker2", "worker3"]
REMOTE_CONF_DIR = "/etc/hadoop"  # 또는 실제 HADOOP_CONF_DIR 확인해서 수정


def backup_file(filepath):
    """
    B 결정: backups/ 디렉토리에 타임스탬프 파일명으로 저장
    TODO: shutil.copy2 활용, backups/ 없으면 os.makedirs
    """
    pass


def load_or_get_property(tree, name):
    """
    TODO: root.find(f".//property[name='{name}']") 로 찾기
    없으면 None 반환 (호출부에서 생성 분기)
    """
    pass


def set_property(tree, name, value):
    """
    TODO:
    - 이미 있으면 <value> 텍스트만 교체
    - 없으면 <property><name>...</name><value>...</value></property> 새로 생성해서
      <configuration> 루트에 append (ET.SubElement 활용)
    """
    pass


def modify_config_file(filepath, target_values):
    """
    A 결정: ElementTree로 파싱 → 수정 → 저장
    TODO: try/except로 FileNotFoundError, ET.ParseError 구분해서 상태 리포트
    """
    pass


def deploy_to_containers(local_config_dir):
    """
    C 결정: docker cp 이중 루프
    TODO: for container in CONTAINERS: for f in TARGET_VALUES: subprocess.run(["docker","cp",...])
    """
    pass


def restart_daemons():
    """
    C 결정: docker exec으로 각 컨테이너의 데몬만 stop/start (컨테이너 자체 재시작 아님)
    TODO: namenode 컨테이너 -> hdfs/yarn daemon 각각, worker 컨테이너 -> datanode/nodemanager daemon
    각 컨테이너 역할에 맞는 daemon 이름 매핑 필요
    """
    pass


def main():
    if len(sys.argv) != 2:
        print("사용법: python3 modify_config.py <config_dir>")
        sys.exit(1)

    config_dir = sys.argv[1]
    # TODO: 1) 백업 2) 수정 3) 배포 4) 재시작, 각 단계 결과를 status report로 출력


if __name__ == "__main__":
    main()