#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${NETEM_DELAY_MS:-}" && "${NETEM_DELAY_MS}" != "0" ]]; then
  tc qdisc replace dev eth0 root netem delay "${NETEM_DELAY_MS}ms"
fi

exec "$@"
