from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,TimestampType

conf = SparkConf()
conf.setAppName("order explicit schema")
conf.setMaster("local[4]")



spark = SparkSession.builder.config(conf = conf).getOrCreate()


# orders_schema  = order_id,order_date,order_customer_id,order_status

order_schema = StructType(
    [
    StructField("orderid",IntegerType()),
    StructField("order_date",TimestampType()),
    StructField("order_customer_id",IntegerType()),
    StructField("order_status",StringType())
    
    ]
)

source_data = spark.read.format("csv")\
        .option("header",True)\
        .schema(order_schema).option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv")\
        .load()

source_data.show()
source_data.printSchema()