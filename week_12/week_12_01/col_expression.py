from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col,concat,expr

conf = SparkConf()
conf.setAppName("column expression")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv").option("inferSchema",True)\
       .option("header",True).option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv")\
       .load()



# expr = source_data.select(col("order_status"),expr("concat(order_status,'_STATUS')").alias("status"))   

# # expr.show()


expr = source_data.selectExpr("order_status","concat(order_status,'_STATUS')")

expr.show()