from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("spark-sql")
conf.setMaster("local[4]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_data = spark.read.format("csv")\
    .option("inferSchema",True).option("header",True)\
    .option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv").load()

# creating spark sql
source_data.createOrReplaceTempView("orders")

spark_sql = spark.sql("select order_customer_id,count(1) from orders group by order_customer_id")

spark.sql("create database if not exists retail")

l1  = spark.catalog.listTables("retail")

print(l1)