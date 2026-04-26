df = spark.read.format("delta").load("output/silver")

df_agg = df.groupBy("product").sum("amount")

df_agg.write.format("delta").mode("overwrite").save("output/gold")
