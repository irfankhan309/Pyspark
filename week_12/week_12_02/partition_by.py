from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("spark partition by")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


# here below for learning purpose once paritioned teh data tried to read the data from different part ie. below some changes are there 

orders_df = spark.read.format("csv").option("inferSchema",True).option("header",True)\
        .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()


# cust_schema   = ('order_id integer,order_date timestamp,order_customer_id integer')

# orders_df = spark.read.format("csv").schema(cust_schema).option("header",True)\
#         .option("path","D:/pyspark_codes/pilot_prepare/week_12/week_12_02/partition_by_data/").load()

# test = orders_df.filter(orders_df.order_id ==115)
# test.show()

# write the orders data in partitioning i.e based on some column distinct values applied the partitionBy
wirte_df = orders_df.write.format("csv").mode("overwrite").partitionBy("order_status")\
            .option("path","D:/pyspark_codes/pilot_prepare/week_12/week_12_02/partition_by_data/").save()

