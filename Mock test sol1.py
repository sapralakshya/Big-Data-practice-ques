from pyspark.sql import SparkSession

if __name__=='__main__':
    spark=SparkSession.builder.appName("Reading data").getOrCreate()
    employeedf=spark.read.option('Header',True).option('InferSchema',True).csv("/spark_data/employees_data")
    employeedf.show(10)
    
