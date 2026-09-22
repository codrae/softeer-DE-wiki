# W2M6 - Docker 이미지를 AWS EC2에 배포하기

W2M5의 분석 환경(JupyterLab + Scrapy/Playwright + 워드클라우드)을 Docker 이미지로 만들어
ECR에 푸시하고 EC2에 배포한 미션입니다.

## 파일 구성

| 파일 | 설명 |
|---|---|
| `Dockerfile` | python:3.11-slim 기반 분석 환경 이미지 |
| `requirements.txt` | jupyterlab, pandas, scrapy, scrapy-playwright, wordcloud 등 |
| `user-data.sh` | EC2 부팅 시 Docker를 자동 설치하는 user data 스크립트 |

## 이미지 구성 포인트

- `fonts-nanum` 설치 — 워드클라우드 한글 렌더링에 필수 (없으면 □ tofu가 그려짐)
- `playwright install --with-deps chromium` — Chromium과 의존 시스템 패키지를 이미지에 포함
- `CMD`로 JupyterLab을 `0.0.0.0:8888`에 토큰/패스워드 없이 기동 (실습 목적)

## 배포 흐름

1. 로컬에서 이미지 빌드
2. ECR 리포지토리에 로그인 후 푸시
3. EC2 인스턴스 생성 시 `user-data.sh`를 User data로 지정
   → 부팅과 동시에 `dnf`로 Docker 설치, `systemctl enable --now docker`,
     `ec2-user`를 docker 그룹에 추가
4. EC2에서 ECR 이미지를 pull 하고 컨테이너 실행 (8888 포트 매핑)

## 팀 논의 내용

### Docker를 쓰는 이유

1. **개발 환경과 실행 환경을 동일하게 맞출 수 있다** — 로컬에서 되는데 서버에서 안 되는
   문제 대부분은 환경 불일치에서 온다. `venv`/`conda`/`uv` 같은 가상환경 도구와 달리
   Docker는 **실행 환경 전체를 이미지로 묶어 배포**한다는 점이 다르다.
2. **여러 머신에 동일 환경을 빠르게 배포** — DE에서는 scale-out으로 여러 머신에 분산
   처리하는 경우가 많은데, 머신마다 환경을 일일이 맞추면 시간이 많이 든다.
3. **버전 관리와 공유가 쉽다** — 롤백이 쉽고 레지스트리로 공유하기 좋다. 프론트/백엔드가
   서로 다른 Java 버전을 요구하는 상황에서도 프로젝트별로 독립 구성이 가능하다.
   `docker compose`로 개발/운영 환경을 분리할 수도 있다.
4. **실행 관리가 편리하다** — `nohup`/`gunicorn`으로 프로세스를 직접 관리하는 것보다
   실행 상태·로그·중지·재시작 관리가 수월하다. 또 같은 시스템을 여러 개 띄울 때
   컨테이너 내부에서는 동일한 포트(`5432`, `8888` 등)를 쓸 수 있다 — 호스트 매핑 포트만
   다르게 주면 된다.

### Docker 사용 시 불편한 점

1. **이미지 크기** — 빌드 방식에 따라 크기가 크게 달라진다. 커지면 빌드·pull·배포 시간이
   길어지고 EC2 등 서버 자원에 부담을 주며, 크기 때문에 실행이 어려워지는 경우도 있다.
2. **디버깅 관점이 늘어난다** — 코드 자체 문제인지, 의존성 문제인지, Dockerfile 작성
   문제인지, 이미지 빌드 문제인지, 컨테이너 실행 옵션·포트 매핑·볼륨 마운트 문제인지
   — "이게 Docker의 문제인가?"라는 관점이 추가된다.

### 여러 대의 EC2에 여러 컨테이너를 배포한다면

이번 미션은 EC2 1대에 컨테이너 1개였다. 규모가 커지면 수동 배포는 한계가 있어
오케스트레이션 플랫폼(**Kubernetes/EKS, Docker Swarm, ECS**)이 필요하다. 추가로 필요한 것:

- **배포 권한 및 체계** — 배포 권한 관리, 이미지 저장소 접근 권한, 환경 변수 및 Secret 관리,
  운영/개발 환경 분리
- **컨테이너 배치 및 Balancing** — CPU·메모리 사용량, 네트워크 트래픽, 컨테이너별 우선순위,
  장애 발생 시 재배치, 부하 분산

## 실행

```bash
docker build -t w2m6-analysis .
docker run -p 8888:8888 w2m6-analysis
# http://localhost:8888
```

> `Dockerfile`이 `notebooks/`와 `data/`를 COPY하므로, 빌드 전에 W2M5의 노트북과 데이터를
> 해당 경로에 준비해야 합니다.
