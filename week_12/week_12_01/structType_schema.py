from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType


conf = SparkConf()
conf.setAppName("structutype schema ")
conf.setMaster("local[4]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

# order_id,order_date,order_customer_id,order_status

struct_schema = StructType(
    [
    StructField("order_id",IntegerType()),
    StructField("order_date",TimestampType()),
    StructField("order_customer_id",IntegerType()),
    StructField("order_status",StringType())
    ]
)


source_data = spark.read.format("csv").schema(struct_schema)\
        .option("header",True).option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv")\
        .load()


source_data.show()
source_data.printSchema()