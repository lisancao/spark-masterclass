"""Step 9: a broker that needs SASL. Write and read as user alice over SASL_PLAINTEXT.

The broker of ch89/kafka.sh has a second listener, localhost:29117, with SASL/PLAIN.
The session sets spark.sql.kafka.disallowedOptions (new in 4.3.0, internal, static), as a
platform team does, so that no query can turn off TLS host name checks.
Run from code/:  uv run python ch89/k9_security.py   (after ch89/kafka.sh start)
"""
import os

from pyspark.sql import functions as F
from k89 import spark_session, work, kafka, publish, slices, trip_rows, err, PLAIN, SASL

spark = spark_session("ch89-security",
                      spark_sql_kafka_disallowedOptions="ssl.endpoint.identification.algorithm")
w = work("ch89/k9")
kafka("topic", "secure_trips", "2")

PASSWORD = os.environ.get("KAFKA_PASSWORD", "alice-secret")      # from a secret store in production
JAAS = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="alice" password="{PASSWORD}";')
secure = {"kafka.bootstrap.servers": SASL,
          "kafka.security.protocol": "SASL_PLAINTEXT",
          "kafka.sasl.mechanism": "PLAIN",
          "kafka.sasl.jaas.config": JAAS}


def read(opts, **extra):
    r = spark.read.format("kafka").option("subscribe", "secure_trips")
    for k, v in {**opts, **extra}.items():
        r = r.option(k, v)
    return r.load()


lines = trip_rows(slices()[:1])
print("1. Write 487 trips over SASL_PLAINTEXT as alice:")
publish(spark, "secure_trips", lines, servers=SASL, opts={k: v for k, v in secure.items() if k != "kafka.bootstrap.servers"})
print("   done")

print("\n2. Read them back as a stream (availableNow), as alice:")
q = (spark.readStream.format("kafka").options(**secure).option("subscribe", "secure_trips")
     .option("startingOffsets", "earliest").load()
     .groupBy("partition").agg(F.count("*").alias("n"))
     .writeStream.format("memory").queryName("secure").outputMode("complete")
     .option("checkpointLocation", str(w / "ck")).trigger(availableNow=True).start())
q.awaitTermination()
print("   rows per partition:", {r.partition: r.n for r in spark.table("secure").collect()})

print("\n3. The same read without credentials, to the SASL port (short timeouts):")
short = {"kafka.request.timeout.ms": "3000", "kafka.default.api.timeout.ms": "6000", "fetchOffset.numRetries": "1"}
try:
    print("   ", read({"kafka.bootstrap.servers": SASL}, **short).count())
except Exception as e:
    print(err(e, 84, 2))

print("\n4. A wrong password:")
bad = dict(secure, **{"kafka.sasl.jaas.config": JAAS.replace(PASSWORD, "wrong")})
try:
    print("   ", read(bad, **short).count())
except Exception as e:
    print(err(e, 84, 3))

print("\n5. The PLAINTEXT port of the same broker needs no login (this is why it must not be public):")
print("   rows:", read({"kafka.bootstrap.servers": PLAIN}).count())

print("\n6. An option that the platform disallowed (spark.sql.kafka.disallowedOptions):")
try:
    read(secure, **{"kafka.ssl.endpoint.identification.algorithm": ""}).count()
except Exception as e:
    print(err(e, 84, 3))
print("   and the setting is static:")
try:
    spark.conf.set("spark.sql.kafka.disallowedOptions", "")
except Exception as e:
    print(err(e, 84, 2))
spark.stop()
