# ControlCharts

Control charts, also known as Shewhart charts (after Walter A. Shewhart) or process-behavior charts, are a statistical process control tool used to determine if a manufacturing or business process is in a state of control.

## Prerequisites:
### Using with Spark shell: 
...
Add databricks:spark-csv to process csv files within spark dataframes
...
This package can be added to Spark using the --packages command line option. For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11:

$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.11:1.5.0

### Spark compiled with Scala 2.10:

$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

Refer https://github.com/databricks/spark-csv for more details
