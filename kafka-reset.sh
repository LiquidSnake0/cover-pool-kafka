#!/usr/bin/env bash
# Empties the log and forgets every bookmark, so the walkthrough starts from
# nothing. `kafka-down.sh --purge` also clears the log, but it destroys the
# container and the next start pays for the broker booting again. This keeps
# the broker up and only resets what the demo writes.
set -euo pipefail

NAME=cover-pool-kafka
TOPIC=loan-events
GROUP=cover-pool-builder
PARTITIONS=3

docker() { command sg docker -c "docker $*"; }
kafka()  { docker "exec $NAME /opt/kafka/bin/$*"; }

if ! docker "ps --format '{{.Names}}'" | grep -qx "$NAME"; then
  echo "Kafka is not running. Start it with ./kafka-up.sh"
  exit 1
fi

# A group with live members cannot be deleted, and a consumer still running
# would immediately recreate the topic by subscribing to it. Saying so beats
# a confusing half-reset.
if kafka "kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
          --describe --group $GROUP --members" 2>/dev/null | grep -q rdkafka; then
  echo "A consumer is still a member of the group."
  echo
  echo "  Stop it with Ctrl+C, which calls consumer.Close() and leaves the group"
  echo "  immediately, then run this again."
  echo
  echo "  If you already killed it another way, the process is gone but the broker"
  echo "  does not know yet: Close() never ran. It notices when the connection"
  echo "  drops, or at the latest after session.timeout.ms. Measured here: about"
  echo "  ten seconds. Wait and retry."
  exit 1
fi

echo "Deleting the topic and every group..."

kafka "kafka-topics.sh --bootstrap-server localhost:9092 \
       --delete --topic $TOPIC" >/dev/null 2>&1 || true

# Every group, not just the default one: the replay demo creates throwaway
# groups named trial-NNNN, and leaving them behind clutters the next run.
for g in $(kafka "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list" \
           2>/dev/null | tr -d '\r'); do
  kafka "kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
         --delete --group $g" >/dev/null 2>&1 || true
done

# Deletion is asynchronous. Recreating too early recreates the topic that is
# still being removed, and the partitions come back with the old data.
printf "Waiting for the deletion to complete"
for _ in $(seq 1 30); do
  if ! kafka "kafka-topics.sh --bootstrap-server localhost:9092 --list" \
       2>/dev/null | grep -qx "$TOPIC"; then
    echo " - done."
    break
  fi
  printf "."
  sleep 1
done

kafka "kafka-topics.sh --bootstrap-server localhost:9092 \
       --create --if-not-exists --topic $TOPIC \
       --partitions $PARTITIONS --replication-factor 1" >/dev/null

echo
kafka "kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $TOPIC"

echo
echo "Empty log, no bookmarks. Start the walkthrough with:"
echo "  dotnet run --project src/CoverPool.Producer"
