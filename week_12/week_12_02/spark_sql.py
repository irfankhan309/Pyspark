from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("spark sql example").setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


orders_df = spark.read.format("csv").option("header",True).option("inferSchema",True)\
            .load("D:/pyspark_codes/pilot_prepare/week_12/orders.csv")

# creating spark sql with table 
orders_df.createOrReplaceTempView("order_tab")


# sample query
# Q1 = spark.sql("select * from order_tab where order_status = 'CLOSED' ").show()

spark.sql("create database if not exists retail_db")

# store_in_tab_df = orders_df.write.format("csv").mode("overwrite").saveAsTable("retail_db.orders")



# tabs = spark.catalog.listTables("retail_db")

# print(tabs)

spark.sql("show tables from default")