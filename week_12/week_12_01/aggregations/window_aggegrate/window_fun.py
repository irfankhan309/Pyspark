from pyspark.sql import SparkSession,Window
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("window functions")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv")\
    .option("inferSchema",True)\
    .option("header",True).option("path","D:/pyspark_codes/order_data.csv").load()

# window  = Window.partitionBy("Country")\
#         .orderBy("customerID")\
#         .rowsBetween(Window.unboundedPreceding, Window.currentRow)

window = Window.orderBy("customerId").partitionBy("Country").rowsBetween(Window.unboundedPreceding, Window.currentRow)

print(dir(Window()))

# source_data.show()

# source_data.printSchema()

mydf = source_data.withColumn("RunningTotal",f.sum("Quantity").over(window)).show()


