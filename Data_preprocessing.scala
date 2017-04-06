import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../data/dataset.csv")

val colum_agg = "YR"
val colum = "IP" //provide the column you want to observe
val df1 = df.groupBy(colum_agg).agg(max("IP").alias("MAX"),min("IP").alias("MIN"),round(avg(colum),2).alias("MEAN"),count(colum).alias("sample"))
val data = df1.withColumn("RANGE",round(($"MAX" - $"MIN"),2))

data.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("../data/processed_data.csv")