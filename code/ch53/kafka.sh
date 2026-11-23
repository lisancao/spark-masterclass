#!/usr/bin/env bash
# One Kafka broker (KRaft mode) in Docker for Chapter 53, with two listeners for Spark on the host:
#   PLAINTEXT       localhost:29116   (no authentication)
#   SASL_PLAINTEXT  localhost:29117   (SASL/PLAIN, users alice and bob, see sasl/)
# ch53/kafka.sh start | stop | topic <name> <partitions> [extra kafka-topics args] | cli <tool> [args]
set -euo pipefail
NAME=sm-v3stream-kafka
IMAGE=apache/kafka:4.0.0
HERE=$(cd "$(dirname "$0")" && pwd)
PLAIN=${KAFKA_PORT:-29116}
SASL=${KAFKA_SASL_PORT:-29117}
case "${1:-}" in
  start)
    docker rm -f $NAME >/dev/null 2>&1 || true
    cfg=$(mktemp -d)
    cat > "$cfg/server.properties" <<EOF
node.id=1
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,HOST://0.0.0.0:$PLAIN,SASL://0.0.0.0:$SASL
advertised.listeners=PLAINTEXT://localhost:9092,HOST://localhost:$PLAIN,SASL://localhost:$SASL
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,SASL:SASL_PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
sasl.enabled.mechanisms=PLAIN
listener.name.sasl.sasl.enabled.mechanisms=PLAIN
listener.name.sasl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_alice="alice-secret" user_bob="bob-secret";
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
log.dirs=/tmp/kraft-logs
EOF
    chmod -R a+rX "$cfg"
    docker run -d --name $NAME -p $PLAIN:$PLAIN -p $SASL:$SASL -v "$cfg:/cfg:ro" --entrypoint bash $IMAGE -c \
      "/opt/kafka/bin/kafka-storage.sh format -t sm-v3stream-kafka-0001 -c /cfg/server.properties >/dev/null && exec /opt/kafka/bin/kafka-server-start.sh /cfg/server.properties" >/dev/null
    for i in $(seq 60); do
      docker exec $NAME /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && { echo "Kafka up: PLAINTEXT localhost:$PLAIN, SASL_PLAINTEXT localhost:$SASL ($IMAGE)"; exit 0; }
      sleep 1
    done
    echo "Kafka did not start" >&2; docker logs --tail 30 $NAME >&2; exit 1 ;;
  stop) docker rm -f $NAME >/dev/null && echo stopped ;;
  topic)
    t=$2; p=$3; shift 3
    docker exec $NAME /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true
    sleep 1
    docker exec $NAME /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$t" --partitions "$p" --replication-factor 1 "$@" >/dev/null && echo "topic $t: $p partitions $*" ;;
  cli)
    tool=$2; shift 2
    docker exec -i $NAME /opt/kafka/bin/$tool.sh --bootstrap-server localhost:9092 "$@" ;;
  *) echo "usage: $0 start|stop|topic <name> <partitions> [args]|cli <tool> [args]" >&2; exit 2 ;;
esac
