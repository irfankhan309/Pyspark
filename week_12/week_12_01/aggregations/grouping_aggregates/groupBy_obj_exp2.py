from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("group by usign object exp")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()



source_data = spark.read.format("csv")\
             .option("inferSchema",True)\
             .option("header",True)\
             .option("path","D:/pyspark_codes/order_data.csv").load()



group_by = source_data.groupBy("InvoiceNo","Country")

aggregate = group_by.agg(
            f.sum("Quantity").alias("Total"),
            f.sum(f.expr("Quantity * 10")).alias("Qty_10")
)

aggregate.show()