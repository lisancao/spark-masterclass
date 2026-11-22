from pyspark.sql import SparkSession
mine = SparkSession.builder.config("spark.sql.shuffle.partitions", "3").getOrCreate()
print("builder returned the pipeline session:", mine is spark,
      "| shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))
