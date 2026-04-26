from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

df.write.format("delta").mode("overwrite").save("output/bronze")
