#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

runtime_root() {
  local value="${QQAI_RUNTIME_ROOT:-}"
  if [[ -z "$value" && -f .env ]]; then
    value="$(
      awk -F= '
        /^[[:space:]]*QQAI_RUNTIME_ROOT[[:space:]]*=/ {
          sub(/^[^=]*=/, "")
          gsub(/^[[:space:]]+|[[:space:]]+$/, "")
          gsub(/^["'"'"']|["'"'"']$/, "")
          print
          exit
        }
      ' .env
    )"
  fi
  value="${value:-./runtime}"
  if [[ "$value" != /* ]]; then
    value="$ROOT/${value#./}"
  fi
  printf '%s\n' "$value"
}

usage() {
  echo "Usage: ./manage.sh prepare|start|stop|status|logs|login|activate-account [qq]|qce-token|console-url|probe [group-id]|purge-cache [--apply]"
}

command="${1:-}"
case "$command" in
  prepare)
    python3 bootstrap.py --runtime-root "$(runtime_root)"
    ;;
  start)
    python3 bootstrap.py --runtime-root "$(runtime_root)"
    docker compose up -d --build
    ;;
  stop)
    docker compose down
    ;;
  status)
    docker compose ps
    ;;
  logs)
    docker compose logs --tail 200 collector-console
    ;;
  login)
    docker compose logs -f napcat-qce
    ;;
  activate-account)
    if [[ -n "${2:-}" ]]; then
      python3 activate_account.py \
        --runtime-root "$(runtime_root)" \
        --account "$2"
    else
      python3 activate_account.py \
        --runtime-root "$(runtime_root)"
    fi
    ;;
  qce-token)
    docker compose exec -T napcat-qce sh -lc \
      'for f in /app/.qq-chat-exporter/security.json /root/.qq-chat-exporter/security.json; do if [ -f "$f" ]; then cat "$f"; exit 0; fi; done; exit 1'
    ;;
  console-url)
    token="$(docker compose exec -T collector-console sh -lc 'cat /data/manager/manager.token')"
    printf 'Use an SSH tunnel, then open:\nhttp://127.0.0.1:%s/?session_token=%s\n' \
      "${COLLECTOR_LOCAL_FORWARD_PORT:-17891}" "$token"
    ;;
  probe)
    group="${2:-}"
    if [[ -n "$group" ]]; then
      docker compose exec -T collector-console \
        python /app/linux_compat_probe.py --group "$group"
    else
      docker compose exec -T collector-console \
        python /app/linux_compat_probe.py
    fi
    ;;
  purge-cache)
    if [[ "${2:-}" == "--apply" ]]; then
      python3 cache_cleanup.py \
        --session-root "$(runtime_root)/qq-session" \
        --keep-days "${QQAI_CACHE_KEEP_DAYS:-1}" \
        --thumbnail-keep-days "${QQAI_CACHE_THUMBNAIL_KEEP_DAYS:-1}" \
        --apply
    else
      python3 cache_cleanup.py \
        --session-root "$(runtime_root)/qq-session" \
        --keep-days "${QQAI_CACHE_KEEP_DAYS:-1}" \
        --thumbnail-keep-days "${QQAI_CACHE_THUMBNAIL_KEEP_DAYS:-1}"
    fi
    ;;
  *)
    usage
    exit 2
    ;;
esac
