# ControlCharts

Control charts, also known as Shewhart charts (after Walter A. Shewhart) or process-behavior charts, are a statistical process control tool used to determine if a manufacturing or business process is in a state of control.

This code helps to create below charts on time-series data, 
1. x-bar and R chart
2. x-bar and s chart 

## Prerequisites:
#### Using with Spark shell: 
Add databricks:spark-csv to process csv files within spark dataframes. This package can be added to Spark using the --packages command line option. For example, to include it when starting the spark shell:

#### Spark compiled with Scala 2.11:

$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.11:1.5.0

#### Spark compiled with Scala 2.10:

$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

Refer https://github.com/databricks/spark-csv for more details

### Steps for executing Data_processing.scala

Note: Make sure that paths are updated in scripts before executing.
1. Provide the column on which data to be grouped in "colum_agg" variable (Ex: here provided "Year")
2. Provide the column on which controlcharts to be processed in "colum" variable (Ex: here provided "Weight")

### Steps for executing Contol_Chart.scala
Note: Make sure that paths are updated in scripts before executing.
Input for this program is, the output generated by Data_processing.scala
