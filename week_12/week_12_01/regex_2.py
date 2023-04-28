from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,StringType,TimestampType

conf = SparkConf()
conf.setAppName("regex 2")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()




source_data = spark.read.text("D:/pyspark_codes/pilot_prepare/week_12/week_12_01/new_orders.csv")


regex = "^(\S+) (\S+)    (\S+)\,(\S+)"

transform = source_data.select(F.regexp_extract("value",regex,1).alias("order_id"),
                               F.regexp_extract("value",regex,2).alias("order_date"),
                               F.regexp_extract("value",regex,3).alias("customer_id"),
                               F.regexp_extract("value",regex,4).alias("order_status")
                               )



order_schema = "order_id Integer, order_date Timestamp, customer_id Integer,order_status String"


conv = transform.withColumn("order_id",transform.order_id.cast(IntegerType()))\
                            .withColumn("order_date",transform.order_date.cast(TimestampType()))\
                            .withColumn("customer_id",transform.customer_id.cast(IntegerType()))\
                            .withColumn("order_status",transform.order_status.cast(StringType()))
                            
conv.show()
conv.printSchema()



# source_data.show()
# source_data.printSchema()