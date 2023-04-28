from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

conf = SparkConf()
conf.setAppName("ddl string schema")
conf.setMaster("local[4]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

# order_id,order_date,order_customer_id,order_status

ddl_string_schema = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"

source_data = spark.read.format("csv").schema(ddl_string_schema)\
            .option("header",True)\
            .option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv")\
            .load()


source_data.show()

source_data.printSchema()

print(source_data.columns)