FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Kolkata \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /app/

COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

RUN addgroup --gid 1001 tts && \
    adduser --uid 1001 --gid 1001 --disabled-password --gecos "" tts && \
    chown -R tts:tts /app

USER tts

EXPOSE 8000

CMD ["sh", "-c", "echo '[TTS] Booting on port '${PORT:-8000}; alembic upgrade head || echo '[TTS] Alembic skipped'; echo '[TTS] Starting uvicorn on port '${PORT:-8000}; exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]
