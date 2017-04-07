//Entry criteria for the script is data with Sample size, Mean, Range
val data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../processed_testdata.csv")
val process_chart = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../sample_size.csv")


//val n_s1 = data.select($"sample".cast("int"))

val n_s1 = data.select($"sample".cast("int"))
val n_s2 = n_s1.agg(avg("sample"))
val n_s3 = n_s2.rdd.map(r => r(0)).collect()
val n_s4 = n_s3(0)
val n = n_s4.asInstanceOf[Number].intValue

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
val temp = data.agg(avg("MEAN"))
val temp1 = temp.select("avg(MEAN)").rdd.map(r => r(0)).collect()
val temp2 = temp1(0)
val sample = temp2.asInstanceOf[Number].doubleValue

//range
val rtemp = data.agg(avg("RANGE"))
val rtemp1 = rtemp.select("avg(RANGE)").rdd.map(r => r(0)).collect()
val rtemp2 = rtemp1(0)
val rsample = rtemp2.asInstanceOf[Number].doubleValue

//Standard_Deviation
val stemp = data.agg(avg("STDDEV"))
val stemp1 = stemp.select("avg(STDDEV)").rdd.map(r => r(0)).collect()
val stemp2 = stemp1(0)
val ssample = stemp2.asInstanceOf[Number].doubleValue

/* xbar -R chart*/

//X-Bar chart UCL and LCL
val LCLrx = sample - (rsample * A2)
val UCLrx = sample + (rsample * A2)
val xrchart = data.withColumn("LCL", round(lit(LCLrx),2)).withColumn("UCL", round(lit(UCLrx),2)).withColumn("Mean", round(lit(sample),2))
val xrbar_chart = xrchart.drop(col("RANGE")).drop(col("MEAN"))
xrbar_chart.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("../xrchart/xbar")
//xbar_chart.repartition(1).write.mode("overwrite").json("../xbar")

//R-Chart UCL and LCL
val LCLr = rsample * D3
val UCLr = rsample * D4
val rchart = data.withColumn("LCL",round(lit(LCLr),2)).withColumn("UCL",round(lit(UCLr),2)).withColumn("R-Bar", round(lit(rsample),2))
val rbar_chart = rchart.drop(col("MEAN"))
rbar_chart.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("../xrchart/rbar")
//rbar_chart.repartition(1).write.mode("overwrite").json("../rbar")

/* xbar -R chart*/
val UCLs = B4 * ssample
val LCLs = B3 * ssample
val schart = data.withColumn("LCL",round(lit(LCLs),2)).withColumn("UCL",round(lit(UCLs),2)).withColumn("S-bar", round(lit(rsample),2))
val sbar_chart = rchart.drop(col("MEAN")).drop(col("RANGE"))
sbar_chart.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("../xschart/sbar")

//A3 is a control chart constant that depends on the subgroup size.
val UCLsx = sample + (A3 * ssample)
val LCLsx = sample - (A3 * ssample)
val xschart = data.withColumn("LCL", round(lit(LCLsx),2)).withColumn("UCL", round(lit(UCLsx),2)).withColumn("Mean", round(lit(sample),2))
val xsbar_chart = xchart.drop(col("RANGE")).drop(col("MEAN"))
xsbar_chart.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("../xschart/xbar")