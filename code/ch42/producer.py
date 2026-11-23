"""A paced Kafka producer with no Python Kafka client: it pipes lines into the broker's own
kafka-console-producer inside the container. Each record gets its CreateTime when it is sent."""
import subprocess
import time

CONTAINER = "sm-part05-kafka"


def produce(topic, rate, seconds, users=40, start_id=0):
    """Send rate records a second for seconds, keys u0..u{users-1}, values = a running id."""
    p = subprocess.Popen(
        ["docker", "exec", "-i", CONTAINER, "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", "localhost:9092", "--topic", topic,
         "--property", "parse.key=true", "--property", "key.separator=,",
         "--producer-property", "linger.ms=0", "--producer-property", "acks=1"],
        stdin=subprocess.PIPE, text=True, bufsize=1)
    t0 = time.time()
    n = 0
    while n < rate * seconds:
        due = t0 + n / rate
        if due > time.time():
            time.sleep(due - time.time())
        p.stdin.write(f"u{n % users},{start_id + n}\n")
        p.stdin.flush()
        n += 1
    p.stdin.close()
    p.wait()
    return n
