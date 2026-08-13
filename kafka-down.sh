#!/usr/bin/env bash
# Stops Kafka. Without --purge the log is kept for the next start.
set -euo pipefail

NAME=cover-pool-kafka

if [[ "${1:-}" == "--purge" ]]; then
  sg docker -c "docker rm -f $NAME" >/dev/null 2>&1 || true
  echo "Container and log removed."
else
  sg docker -c "docker stop $NAME" >/dev/null 2>&1 || true
  echo "Kafka stopped. The log is kept - ./kafka-up.sh to resume."
fi
