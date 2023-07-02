from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__=='__main__':
    spark=SparkSession.builder.appName("Count Revenue").getOrCreate()
    Salesdf=spark.read.option('Header',True).option('InferSchema',True).csv("/sparks_data/Sales_data.txt")
    Salesdf.groupBy('Product_name').agg(sum('Revenue').alias('Total_revenue')).orderBy(col("Total_revenue").desc()).show()
