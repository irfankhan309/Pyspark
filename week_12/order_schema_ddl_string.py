from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

conf = SparkConf()
conf.setAppName("order_scheam using ddl string")
conf.setMaster("local[4]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

# order_id,order_date,order_customer_id,order_status


order_ddl_schema = "order_id Integer, order_date Timestamp, order_customer_id Integer,order_status String"

source_data = spark.read.format("csv")\
    .option("header",True)\
    .schema(order_ddl_schema).option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv")\
    .load()


order_status = source_data.withColumn("order_status2",F.concat(F.col("order_status"),F.lit("_STATUS")))

save = order_status.write.format("csv")\
        .option("mode","overwrite").option("maxRecordsPerFile",2000)\
        .partitionBy("order_status2")\
        .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders_data")\
        .save()