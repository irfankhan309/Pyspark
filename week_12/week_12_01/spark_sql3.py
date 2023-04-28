# create a sparksession
# read data from file 
# create a database in spark sql
# create table on dataframe
# store the table data which is some query to fetch


from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark confs
conf = SparkConf()
conf.setAppName("spark sql 3 ex")
conf.setMaster("local[5]")

# spark session creation
spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_data = spark.read.format("csv").option("inferSchema",True)\
            .option("header",True).option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv")\
            .load()


# creating a database in spark sql
spark.sql("create database if not exists orders_04_db")


# creating a table on above dataframe 
source_data.createOrReplaceTempView("orders_04_tab")

q1 = spark.sql("select order_id,order_customer_id,order_status from orders_04_tab")


# now stooring the above query result to new table in above created database orders_04_db
q1.write.format("csv").option("mode","overwrite")\
    .partitionBy("order_status")\
    .bucketBy(4,"order_id").saveAsTable("orders_04_db.orders_04_tab")


q1.show()
q1.printSchema()