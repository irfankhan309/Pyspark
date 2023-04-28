from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

conf = SparkConf()
conf.setAppName("handling ambiguity issues")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


customer_df = spark.read.format("csv")\
             .option("inferSchema",True).option("header",True)\
             .option("path","D:/pyspark_codes/customers.csv").load()


orders_df = spark.read.format("csv").option("inferSchema",True).option("header",True)\
             .option("path","D:/pyspark_codes/orders_2.csv").load()


# rename the colun of orders_df column customer_id to new_customer_id
rename_orders_df = orders_df.withColumnRenamed("customer_id","new_customer_id")

# this is join condition
join_condidtion = customer_df.customer_id == rename_orders_df.new_customer_id


# setting autobroadcast join to false
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# # join the two tables
join_df = customer_df.join(broadcast(rename_orders_df),join_condidtion,"inner").explain()

    # filter_df = join_df.select("order_id","customer_fname","customer_lname","customer_id","new_customer_id","order_status")


    # # filter_df.show()
    # # check_null = filter_df.filter(filter_df.order_id.isNull())


    # # handling nulll here with filter data
    # null_handler = filter_df.withColumn("order_id",F.expr("coalesce(order_id,-1)"))

    # null_handler.show()
