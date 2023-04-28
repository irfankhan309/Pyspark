from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import StorageLevel


conf = SparkConf()
conf.setAppName("first-test")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

order_schema = "order_id int,order_date string, order_customer_id int, order_status string"

source_data = spark.read.format("csv")\
    .option("inferSchema",True)\
    .option("header",True)\
    .option("mode","DROPMALFARMED")\
    .schema(order_schema)\
    .option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv")\
    .load()


# process = source_data.where("order_status = 'COMPLETE' ").select("order_customer_id","order_date","order_status")\
#             .groupBy("order_date").

# process = source_data.where("order_status = 'COMPLETE' ").select("order_customer_id","order_date","order_status")\
#             .persist(StorageLevel.MEMORY_ONLY)



process = source_data.where("order_status = 'COMPLETE' ").select("order_customer_id","order_date","order_status")

# print(dir(process))
# result = process.show(100)
# source_data.printSchema()
# print(result)


write_api = process.write.format("csv")\
            .option("mode","overwrite").option("path","D:/pyspark_codes/pilot_prepare/week_11/customers/").save()

