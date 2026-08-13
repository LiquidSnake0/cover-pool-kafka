#!/usr/bin/env bash
# Starts a single-node Kafka in KRaft mode (no Zookeeper) and creates the topic.
set -euo pipefail

NAME=cover-pool-kafka
TOPIC=loan-events
PARTITIONS=3

docker() { command sg docker -c "docker $*"; }

if docker "ps -a --format '{{.Names}}'" | grep -qx "$NAME"; then
  echo "Container exists, restarting..."
  docker "start $NAME" >/dev/null
else
  echo "Starting Kafka..."
  docker "run -d --name $NAME \
    -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_HEAP_OPTS='-Xmx512M -Xms256M' \
    apache/kafka:latest" >/dev/null
fi

printf "Waiting for the broker"
for _ in $(seq 1 45); do
  if docker "exec $NAME /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list" \
       >/dev/null 2>&1; then
    echo " - ready."
    break
  fi
  printf "."
  sleep 2
done

docker "exec $NAME /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic $TOPIC \
  --partitions $PARTITIONS --replication-factor 1" >/dev/null 2>&1 || true

echo
docker "exec $NAME /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic $TOPIC"
