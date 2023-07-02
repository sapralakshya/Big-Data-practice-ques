from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__=='__main__':
    spark=SparkSession.builder.appName("average transactions").getOrCreate()
    logsdf=spark.read.option('Header',True).option('InferSchema',True).csv("/sparks_data/logs_data")
    logsdf.createOrReplaceTempView("logs")
    spark.sql("select hour(timestamp) as hours,count(event) from logs group by hours order by hours")
