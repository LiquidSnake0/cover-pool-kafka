#!/usr/bin/env bash
# Arrête Kafka. Sans --purge, le journal est conservé pour le prochain démarrage.
set -euo pipefail

NOM=cover-pool-kafka

if [[ "${1:-}" == "--purge" ]]; then
  sg docker -c "docker rm -f $NOM" >/dev/null 2>&1 || true
  echo "Conteneur et journal supprimés."
else
  sg docker -c "docker stop $NOM" >/dev/null 2>&1 || true
  echo "Kafka arrêté. Le journal est conservé — ./kafka-up.sh pour reprendre."
fi
