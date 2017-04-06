//Entry criteria for the script is data with Sample size, Mean, Range
val process_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../processed_data.csv")
val process_chart = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../sample_size.csv")


val n_s1 = process_data.select($"sample".cast("int"))
val n_s2 = n_s1.agg(avg("sample"))
val n_s3 = n_s2.rdd.map(r => r(0)).collect()
val n_s4 = n_s3(0)
val n = n_s4.asInstanceOf[Number].integerValue

val test = process_chart.filter($"Sample_Size" === n)

val A2_s1 = test.select($"A2".cast("double"))
val A2_s2 = A2_s1.select("A2").rdd.map(r => r(0)).collect()
val A2_s3 = A2_s2(0)
val A2 = A2_s3.asInstanceOf[Number].doubleValue

val A3_s1 = test.select($"A3".cast("double"))
val A3_s2 = A3_s1.select("A3").rdd.map(r => r(0)).collect()
val A3_s3 = A3_s2(0)
val A3 = A3_s3.asInstanceOf[Number].doubleValue
//d2
val d2_s1 = test.select($"d2".cast("double"))
val d2_s2 = d2_s1.select("d2").rdd.map(r => r(0)).collect()
val d2_s3 = d2_s2(0)
val d2 = d2_s3.asInstanceOf[Number].doubleValue
//D3
val D3_s1 = test.select($"D3".cast("double"))
val D3_s2 = D3_s1.select("D3").rdd.map(r => r(0)).collect()
val D3_s3 = D3_s2(0)
val D3 = D3_s3.asInstanceOf[Number].doubleValue
//D4
val D4_s1 = test.select($"D4".cast("double"))
val D4_s2 = D4_s1.select("D4").rdd.map(r => r(0)).collect()
val D4_s3 = D4_s2(0)
val D4 = D4_s3.asInstanceOf[Number].doubleValue
//B3
val B3_s1 = test.select($"B3".cast("double"))
val B3_s2 = B3_s1.select("B3").rdd.map(r => r(0)).collect()
val B3_s3 = B3_s2(0)
val B3 = B3_s3.asInstanceOf[Number].doubleValue
//B4
val B4_s1 = test.select($"B4".cast("double"))
val B4_s2 = B4_s1.select("B4").rdd.map(r => r(0)).collect()
val B4_s3 = B4_s2(0)
val B4 = B4_s3.asInstanceOf[Number].doubleValue
//mean transformation
val temp = process_data.agg(sum("MEAN"))
val temp1 = temp.select("sum(MEAN)").rdd.map(r => r(0)).collect()
val temp2 = temp1(0)
val temp3 = temp2.asInstanceOf[Number].doubleValue

val tot_row = process_data.count()
val sample = temp3/tot_row

//range
val rtemp = process_data.agg(sum("RANGE"))
val rtemp1 = rtemp.select("sum(RANGE)").rdd.map(r => r(0)).collect()
val rtemp2 = rtemp1(0)
val rtemp3 = rtemp2.asInstanceOf[Number].doubleValue

val rsample = rtemp3/tot_row

//X-Bar chart UCL and LCL
val xLCL = sample - (rsample * A2)
val xUCL = sample + (rsample * A2)

//R-Chart UCL and LCL
val rLCL = D3
val rUCL = rsample * D4

val xchart = process_data.withColumn("LCL", round(lit(xLCL),2)).withColumn("UCL", round(lit(xUCL),2))
val xbar_chart = xchart.drop(col("RANGE"))
//xbar_chart.repartition(1).write.mode("overwrite").json("/user/hive/sample/xbar")
xbar_chart.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("/user/hive/sample/xbar1")

val rchart = process_data.withColumn("LCL",round(lit(rLCL),2)).withColumn("UCL",round(lit(rUCL),2)).withColumn("R-Bar", round(lit(rsample),2))
val rbar_chart = rchart.drop(col("MEAN"))
//rbar_chart.repartition(1).write.mode("overwrite").json("/user/hive/sample/rbar")
rbar_chart.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("/user/hive/sample/rbar1")