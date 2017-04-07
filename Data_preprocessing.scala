import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../testdata.csv")

//pass these values through UI
val colum_agg = "Year"
val colum = "Weight"

val df1 = df.groupBy(colum_agg).agg(max(colum).cast("double").alias("MAX"),min(colum).cast("double").alias("MIN"),round(avg(colum),2).cast("double").alias("MEAN"),count(colum_agg).alias("sample"),round(stddev(colum),2).alias("STDDEV"))
val data = df1.withColumn("RANGE",round(($"MAX" - $"MIN"),2))

data.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("../process_data")