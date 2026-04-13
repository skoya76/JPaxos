#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: jpaxos.sh <replica|client|multiclient> [args...]" >&2
  exit 1
fi

mode="$1"
shift

JAVA_FLAGS=(
  -ea
  -server
  -Dlogback.configurationFile=/opt/jpaxos/logback.xml
  -cp
  /opt/jpaxos/bin:/opt/jpaxos/lib/logback-classic-1.2.3.jar:/opt/jpaxos/lib/logback-core-1.2.3.jar:/opt/jpaxos/lib/slf4j-api-1.7.26.jar
)

case "${mode}" in
  replica)
    if [[ $# -ne 1 ]]; then
      echo "Usage: jpaxos.sh replica <replicaId>" >&2
      exit 1
    fi
    exec java "${JAVA_FLAGS[@]}" lsr.paxos.test.EchoService "$1"
    ;;
  client)
    if [[ $# -ne 2 ]]; then
      echo "Usage: jpaxos.sh client <requestSize> <randomEachRequest>" >&2
      exit 1
    fi
    exec java "${JAVA_FLAGS[@]}" lsr.paxos.test.GenericClient "$1" "$2"
    ;;
  multiclient)
    if [[ $# -ne 2 ]]; then
      echo "Usage: jpaxos.sh multiclient <requestSize> <randomEachRequest>" >&2
      exit 1
    fi
    exec java "${JAVA_FLAGS[@]}" lsr.paxos.test.GenericMultiClient "$1" "$2"
    ;;
  *)
    echo "Unknown mode: ${mode}" >&2
    exit 1
    ;;
esac
