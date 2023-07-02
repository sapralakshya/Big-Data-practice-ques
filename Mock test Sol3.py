from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__=='__main__':
    spark=SparkSession.builder.appName("Reading Json data").getOrCreate()
    studentdf=spark.read.json("/updated_json/Students_data.json")
    studentdf.filter("Student_age>18").select(col("Student_name"),col("Student_age"),col("Grades")).show()
