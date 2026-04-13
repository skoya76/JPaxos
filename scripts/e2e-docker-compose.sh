#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/e2e/docker-compose.yml"

NETEM_DELAY_MS="${NETEM_DELAY_MS:-50}"
REQUEST_COUNT="${REQUEST_COUNT:-50}"
REQUEST_SIZE="${REQUEST_SIZE:-64}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-120}"
KEEP_UP="${KEEP_UP:-0}"

export NETEM_DELAY_MS

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

cleanup() {
  if [[ "${KEEP_UP}" == "1" ]]; then
    echo "Keeping containers up (KEEP_UP=1)." >&2
    return
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local deadline=$((SECONDS + WAIT_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if compose exec -T client bash -e -lc "echo >/dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "[E2E] Timeout waiting for ${host}:${port}" >&2
  return 1
}

wait_for_log() {
  local pattern="$1"
  local deadline=$((SECONDS + WAIT_TIMEOUT_SEC))
  local logs
  while (( SECONDS < deadline )); do
    logs="$(compose logs --no-color 2>/dev/null || true)"
    if grep -q "${pattern}" <<<"${logs}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_replica_execution() {
  local replica="$1"
  local deadline=$((SECONDS + WAIT_TIMEOUT_SEC))
  local logs
  while (( SECONDS < deadline )); do
    logs="$(compose logs --no-color "${replica}" 2>/dev/null || true)"
    if grep -q "Executed request no." <<<"${logs}"; then
      return 0
    fi
    sleep 1
  done
  echo "[E2E] Timeout waiting for execution log on ${replica}" >&2
  return 1
}

trap cleanup EXIT

echo "[E2E] Starting 3 replicas + 1 client with ${NETEM_DELAY_MS}ms one-way netem delay"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d --build replica0 replica1 replica2 client

echo "[E2E] Step 1/3: waiting for replica client ports"
wait_for_port replica0 3001
wait_for_port replica1 3002
wait_for_port replica2 3003
echo "[E2E] Step 1/3 passed: all replica client ports are reachable"

echo "[E2E] Step 2/3: sending client requests"
CLIENT_OUTPUT="$(
  printf "0 %s false\nbye\n" "${REQUEST_COUNT}" | \
    compose exec -T client /opt/jpaxos/docker/e2e/jpaxos.sh client "${REQUEST_SIZE}" false 2>&1
)"
if ! grep -q "Finished" <<<"${CLIENT_OUTPUT}"; then
  echo "[E2E] Client output did not contain expected completion marker." >&2
  echo "${CLIENT_OUTPUT}" >&2
  exit 1
fi

for replica in replica0 replica1 replica2; do
  if ! wait_for_replica_execution "${replica}"; then
    echo "[E2E] ${replica} did not execute any request within timeout." >&2
    exit 1
  fi
done
echo "[E2E] Step 2/3 passed: client completed and all replicas executed requests"

echo "[E2E] Step 3/3: waiting for Dynatune updates in logs"
if ! wait_for_log "Dynatune updated E_t"; then
  echo "[E2E] Dynatune E_t update log was not observed within timeout." >&2
  exit 1
fi
if ! wait_for_log "Dynatune updated suggested heartbeat interval"; then
  echo "[E2E] Dynatune heartbeat interval update log was not observed within timeout." >&2
  exit 1
fi
if ! wait_for_log "Dynatune applied per-follower heartbeat interval"; then
  echo "[E2E] Leader-side per-follower interval application was not observed within timeout." >&2
  exit 1
fi
echo "[E2E] Step 3/3 passed: Dynatune tuning updates were observed"

echo "[E2E] SUCCESS"
