from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import  functions as F


conf = SparkConf()
conf.setAppName("spark simple aggregations")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


orders_df = spark.read.format("csv").option("header",True).option("inferScehma",True)\
            .load("D:/pyspark_codes/pilot_prepare/week_12/order_data.csv")


group_agg = orders_df.groupBy("Country","InvoiceNo")\
                            .agg(F.max("Quantity").alias("max_quantity"),
                                 F.sum("Quantity").alias("total_quantity"),
                                 F.sum(F.expr("Quantity * UnitPrice")).alias("value")
                            )

group_agg.show()

