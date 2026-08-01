#!/usr/bin/env bash
set -euo pipefail

container="${1:-qqai-napcat}"
seconds="${2:-60}"
domains=(ss.xingzhige.com secret-service.bietiaop.com)

echo "This is a bounded observation window. No observed connection does not prove that a cached path never connects."
echo "Run it while Test A is actively receiving images and while a native rkey refresh is expected."
echo

echo "blocked-domain resolution inside ${container}:"
for domain in "${domains[@]}"; do
  docker exec "$container" getent hosts "$domain" || true
done

pid="$(docker inspect -f '{{.State.Pid}}' "$container")"
ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container")"
echo
echo "current TCP sockets in the NapCat network namespace:"
nsenter -t "$pid" -n ss -Hntp || true

if command -v tcpdump >/dev/null 2>&1 && [[ -n "$ip" ]]; then
  echo
  echo "DNS and TCP-SYN metadata for ${seconds}s (no payload capture):"
  timeout "$seconds" tcpdump -nn -q -i any \
    "host $ip and (port 53 or (tcp[tcpflags] & tcp-syn != 0))" || true
else
  echo
  echo "tcpdump is unavailable; only the socket snapshot was collected."
fi
