FROM python:3.11-slim

# 나눔폰트(W2 워드클라우드) + Playwright 실행에 필요한 시스템 라이브러리
RUN apt-get update && \
    apt-get install -y --no-install-recommends fonts-nanum && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Chromium 브라우저 + 의존 시스템 패키지를 이미지에 포함
RUN playwright install --with-deps chromium

COPY notebooks/ ./notebooks/
COPY data/ ./data/

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=", "--NotebookApp.password="]