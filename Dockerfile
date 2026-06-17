FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ttc_stream ./ttc_stream
COPY sql ./sql
COPY tests ./tests
