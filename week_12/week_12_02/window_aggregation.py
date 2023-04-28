from pyspark.sql import SparkSession,Window 
from pyspark import SparkConf
from pyspark.sql import  functions as F


conf = SparkConf()
conf.setAppName("spark simple aggregations")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


orders_df = spark.read.format("csv").option("header",True).option("inferScehma",True)\
            .load("D:/pyspark_codes/pilot_prepare/week_12/order_data.csv")


# mywindow = Window.partitionBy("Country").orderBy("CustomerID")
window = Window.orderBy("customerId").partitionBy("Country").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# print(dir(Window()))

# source_data.show()

# source_data.printSchema()

mydf = orders_df.withColumn("RunningTotal",F.sum("Quantity").over(window)).show(500)


