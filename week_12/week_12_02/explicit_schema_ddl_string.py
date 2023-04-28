from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark confs
conf = SparkConf()
conf.setAppName("spark explict schem using ddl string")
conf.setMaster("local[5]")

# create spark session
spark = SparkSession.builder.config(conf = conf).getOrCreate()

# createing explicit schema using DDL string
orders_schema = 'order_id integer,order_date timestamp,order_customer_id integer,order_status integer'

# creating spark dataframe
# orders_df  = spark.read.format("csv").option("inferSchema",True).option("header",True)\
#              .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()


# orders_df.show()
# orders_df.printSchema()

orders_df  = spark.read.format("csv").schema(orders_schema).option("header",True)\
             .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()


orders_df.show()
orders_df.printSchema()