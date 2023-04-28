from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("simpel agg with sql/string expr")
conf.setMaster("local[5]")

spark  = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv")\
            .option("inferSchema",True).option("header",True).option("path","D:/pyspark_codes/order_data.csv")\
            .load()


source_data.createOrReplaceTempView("orders")

q1 = spark.sql("select count(1),sum(Quantity),max(Quantity),avg(Quantity) from orders")

q1.show()