from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType


# spark config
conf = SparkConf()
conf.setAppName("spark explicit schema")
conf.setMaster("local[5]")


# spark session ccreations
spark = SparkSession.builder.config(conf = conf).getOrCreate()

# spark parameters
seperator = ','
path = 'D:/pyspark_codes/pilot_prepare/week_12/orders.csv'
format = 'csv'
header = True
inferschema = True

# we have two ways to define schema explicitly
# 1.structType
# 2.ddl string


# 1.structType
orders_schema = StructType(
    [
    StructField("order_id",IntegerType()),
    StructField("order_date",TimestampType()),
    StructField("order_customer_id",IntegerType()),
    StructField("order_status",StringType())
    ]
)

# here we create df with inferSchema where some feilds are not infered properly i.e why below we use explict schema in df
# orders_df = spark.read.format(format).option("header",header).option("inferSchema",inferschema).option("sep",seperator)\
#             .load(path)

# orders_df.show()
# orders_df.printSchema()


orders_df = spark.read.format(format).option("header",header).schema(orders_schema).option("sep",seperator)\
            .load(path)


orders_df.show()
orders_df.printSchema()