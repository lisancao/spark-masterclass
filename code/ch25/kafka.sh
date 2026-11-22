#!/usr/bin/env bash
# One Kafka broker (KRaft mode) in Docker for Part V. Spark runs on the host and reaches it
# at localhost:29055.   ch25/kafka.sh start | stop | topic <name> <partitions> [LogAppendTime]
set -euo pipefail
NAME=sm-part05-kafka
IMAGE=apache/kafka:4.0.0
PORT=${KAFKA_PORT:-29055}
case "${1:-}" in
  start)
    docker rm -f $NAME >/dev/null 2>&1 || true
    docker run -d --name $NAME -p $PORT:$PORT \
      -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,HOST://0.0.0.0:$PORT \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,HOST://localhost:$PORT \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
      $IMAGE >/dev/null
    for i in $(seq 60); do
      docker exec $NAME /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && { echo "Kafka up: localhost:$PORT ($IMAGE)"; exit 0; }
      sleep 1
    done
    echo "Kafka did not start" >&2; exit 1 ;;
  stop) docker rm -f $NAME >/dev/null && echo stopped ;;
  topic)
    docker exec $NAME /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$2" >/dev/null 2>&1 || true
    sleep 1
    extra=(); [[ "${4:-}" == LogAppendTime ]] && extra=(--config message.timestamp.type=LogAppendTime)
    docker exec $NAME /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$2" --partitions "$3" --replication-factor 1 "${extra[@]}" >/dev/null && echo "topic $2: $3 partitions ${4:-}" ;;
  *) echo "usage: $0 start|stop|topic <name> <partitions> [LogAppendTime]" >&2; exit 2 ;;
esac
