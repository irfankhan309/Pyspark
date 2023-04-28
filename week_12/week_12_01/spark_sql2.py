# create spark session with hive enable support to store the data in table
# create a database
# create table 
# apply some transform on above table 
# store it in paritioned and bucketed table 


from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("spark sql2")
conf.setMaster("local[5]")

# create spark session with hive support to store the meta data in hive metastore
spark = SparkSession.builder.config(conf = conf).getOrCreate()


# read data file 
source_data = spark.read.format("csv").option("inferSchema",True)\
            .option("header",True)\
            .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()



# creating a daabase
spark.sql("create database if not exists retail_db_03")

source_data.createOrReplaceTempView("orders_new_01")

q1 = spark.sql("select order_customer_id,order_date,order_status from orders_new_01")

q1.write.format("csv").option("mode","overwrite").partitionBy("order_status")\
  .bucketBy(3,"order_customer_id").saveAsTable("retail_db_03.orders_dump_01")




spark.catalog.listTables("retail_db_03")

source_data.show()
source_data.printSchema()