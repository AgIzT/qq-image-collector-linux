FROM node:22-alpine AS frontend-build

WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/index.html frontend/tsconfig.json frontend/tsconfig.app.json \
     frontend/tsconfig.node.json frontend/vite.config.ts ./
COPY frontend/src ./src
RUN npm run build


FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    QQ_IMAGE_CONSOLE_HOME=/data/manager \
    QQ_IMAGE_COLLECTOR_CONFIG=/data/qq-image-collector/config/collector_config.json \
    TZ=Asia/Shanghai

WORKDIR /app
COPY requirements-console.txt /app/requirements-console.txt
RUN python -m pip install --no-cache-dir -r /app/requirements-console.txt

COPY collector.py collector_control.py metadata_reader.py /app/
COPY qq_image_collector /app/qq_image_collector
COPY qq_image_console /app/qq_image_console
COPY linux/cache_cleanup.py linux/event_probe.py linux/diagnostic_compare.py \
     linux/url_lifecycle_probe.py linux/telemetry_report.py /app/linux/
COPY --from=frontend-build /build/frontend/dist /app/frontend/dist

STOPSIGNAL SIGTERM
CMD ["python", "-m", "qq_image_console.main", "--config", "/data/manager/manager_config.json", "--autostart", "--no-browser"]
