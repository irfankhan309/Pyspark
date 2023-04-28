from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("spark sql example 2").setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


orders_df = spark.read.format("csv").option("inferSchema",True).option("header",True)\
             .option("mode","dropmalformed").load("D:/pyspark_codes/pilot_prepare/week_12/orders.csv")


# creating sparkk sql
# orders_df.createOrReplaceTempView("new_orders")

spark.sql("create database if not exists retail_2")

# spark.sql("use retail_2")

# write_df  = orders_df.write.format("csv").option("mode","overwrite")\
#             .saveAsTable("retail_2.new_orders_2")


# list = spark.catalog.listTables("retail_2")

# print(list)


# save the data in table
store_in_tab_df = orders_df.write.format("csv").mode("overwrite")\
                  .bucketBy(4,"order_id").sortBy("order_customer_id")\
                  .saveAsTable("retail_2.orders")
