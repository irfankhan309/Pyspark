from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("spark sql")
conf.setMaster("local[4]")



spark = SparkSession.builder.config(conf = conf).enableHiveSupport().getOrCreate()

source_data = spark.read.format("csv").option("inferSchema",True)\
        .option("header",True).option("mode","DROPMALLFARMED")\
        .option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv")\
        .load()

# creating database
spark.sql("create database if not exists retail_db_02")


# create spark sql
source_data.createOrReplaceTempView("orders")

# query for grooup by data
query1  = spark.sql("select order_status,count(*) from orders group by order_status")

# storing the data in tables of new created database
source_data.write.format("csv").option("mode","overwite")\
        .bucketBy(4,"order_customer_id").sortBy("order_customer_id")\
        .saveAsTable("retail_db_02.orders_04")

# list
spark.catalog.listTables("retail_db_02")


query1.show()






# source_data.show()
# source_data.printSchema()