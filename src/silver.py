df = spark.read.format("delta").load("output/bronze")

df_clean = df.dropna()

df_clean.write.format("delta").mode("overwrite").save("output/silver")
