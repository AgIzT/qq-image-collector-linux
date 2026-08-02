#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

runtime_root() {
  local value="${QQAI_RUNTIME_ROOT:-}"
  if [[ -z "$value" && -f .env ]]; then
    value="$(awk -F= '/^[[:space:]]*QQAI_RUNTIME_ROOT[[:space:]]*=/ {sub(/^[^=]*=/, ""); gsub(/^[[:space:]]+|[[:space:]]+$/, ""); gsub(/^['"'"']|['"'"']$/, ""); print; exit}' .env)"
  fi
  value="${value:-./runtime}"
  [[ "$value" == /* ]] || value="$ROOT/${value#./}"
  printf '%s\n' "$value"
}

usage() {
  echo "Usage: ./manage.sh prepare|start|stop|restart|status|logs|login|activate-account [qq]|console-url|probe-event [group-id] [segments]|diagnose-original <filename> [group-id] [sender-id]|diagnose-metadata <filename> [group-id] [sender-id]|url-lifecycle-capture [group-id]|url-lifecycle-check <label> [--finalize]|telemetry [hours]|audit-rkey-network [seconds]|purge-cache"
}

case "${1:-}" in
  prepare)
    python3 bootstrap.py --runtime-root "$(runtime_root)"
    ;;
  start)
    python3 bootstrap.py --runtime-root "$(runtime_root)"
    docker compose up -d --build --remove-orphans
    ;;
  stop)
    docker compose down --remove-orphans
    ;;
  restart)
    python3 bootstrap.py --runtime-root "$(runtime_root)"
    docker compose up -d --build --force-recreate --remove-orphans
    ;;
  status)
    docker compose ps
    echo
    ss -lnt | grep -E ':(10058|16099|17890|18080|3000|3001|40653)\b' || true
    ;;
  logs)
    docker compose logs --tail 250 collector-console cache-cleaner
    ;;
  login)
    docker compose logs -f napcat
    ;;
  activate-account)
    if [[ -n "${2:-}" ]]; then
      python3 activate_account.py --runtime-root "$(runtime_root)" --account "$2"
    else
      python3 activate_account.py --runtime-root "$(runtime_root)"
    fi
    docker compose restart napcat
    ;;
  console-url)
    token="$(docker compose exec -T collector-console sh -lc 'cat /data/manager/manager.token')"
    printf 'Open through the existing Nginx public endpoint:\nhttp://<server>:18080/?session_token=%s\n' "$token"
    ;;
  probe-event)
    args=()
    [[ -z "${2:-}" ]] || args+=(--group "$2")
    args+=(--image-segments "${3:-200}")
    docker compose exec -T -e PYTHONPATH=/app collector-console \
      python /app/linux/event_probe.py "${args[@]}"
    ;;
  diagnose-original)
    [[ -n "${2:-}" ]] || { echo "diagnostic source filename is required" >&2; exit 2; }
    args=(--source "/diagnostics/$2" --allow-get-image-diagnostic)
    [[ -z "${3:-}" ]] || args+=(--group "$3")
    [[ -z "${4:-}" ]] || args+=(--sender "$4")
    docker compose exec -T -e PYTHONPATH=/app collector-console \
      python /app/linux/diagnostic_compare.py "${args[@]}"
    ;;
  diagnose-metadata)
    [[ -n "${2:-}" ]] || { echo "diagnostic source filename is required" >&2; exit 2; }
    args=(--source "/diagnostics/$2")
    [[ -z "${3:-}" ]] || args+=(--group "$3")
    [[ -z "${4:-}" ]] || args+=(--sender "$4")
    docker compose exec -T -e PYTHONPATH=/app collector-console \
      python /app/linux/diagnostic_compare.py "${args[@]}"
    ;;
  url-lifecycle-capture)
    args=(capture --urls 10)
    [[ -z "${2:-}" ]] || args+=(--group "$2")
    docker compose exec -T -e PYTHONPATH=/app collector-console \
      python /app/linux/url_lifecycle_probe.py "${args[@]}"
    ;;
  url-lifecycle-check)
    [[ -n "${2:-}" ]] || { echo "lifecycle label is required (for example T+1h)" >&2; exit 2; }
    args=(check --label "$2")
    [[ "${3:-}" != "--finalize" ]] || args+=(--finalize)
    docker compose exec -T -e PYTHONPATH=/app collector-console \
      python /app/linux/url_lifecycle_probe.py "${args[@]}"
    ;;
  telemetry)
    docker compose exec -T -e PYTHONPATH=/app collector-console \
      python /app/linux/telemetry_report.py --hours "${2:-72}"
    ;;
  audit-rkey-network)
    ./audit_rkey_network.sh qqai-napcat "${2:-60}"
    ;;
  purge-cache)
    docker compose exec -T -e PYTHONPATH=/app cache-cleaner python /app/linux/cache_cleanup.py \
      --session-root /cleanup/qq-session \
      --napcat-log-root /cleanup/napcat-logs \
      --collector-temp-root /cleanup/repository/temp \
      --collector-state-root /cleanup/repository/state \
      --legacy-qce-root /cleanup/qce-data \
      --short-keep-hours 2 --media-keep-hours 24 --log-keep-hours 48 \
      --legacy-keep-hours 168 --apply
    ;;
  *)
    usage
    exit 2
    ;;
esac
