from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__=='__main__':
    spark=SparkSession.builder.appName("average transactions").getOrCreate()
    transactiondf=spark.read.option('Header',True).option('InferSchema',True).csv("/sparks_data/transaction_data.txt")
    transactiondf.groupBy(col("user_id")).agg(avg("amount").alias("Avg_amt")).show()
 
    
