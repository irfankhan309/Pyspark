from pyspark.sql import SparkSession

from pyspark import SparkConf
from pyspark.sql import functions

conf = SparkConf()
conf.setAppName("sql")
conf.setMaster("local[4]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_data = spark.read.format("csv")\
            .option("header",True).option("inferSchema",True)\
            .option("path","D:/pyspark_codes/pilot_prepare/week_11/orders.csv").load()



# spark_sql = source_data.createOrReplaceTempView("orders")

# q1 = spark.sql("select order_date from orders")

select1 = source_data.select("order_id","order_status")

select2 = source_data.selectExpr("order_id","concat(order_status,'_STATUS')")

select2.show()
