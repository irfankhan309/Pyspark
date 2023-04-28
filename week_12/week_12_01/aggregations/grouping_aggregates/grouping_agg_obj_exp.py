from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("grouping agg with object exp")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_data = spark.read.format("csv")\
              .option("header", "true")\
             .option("inferSchema", "true")\
             .option("path","D:/pyspark_codes/order_data.csv").load()


# root
#  |-- InvoiceNo: string (nullable = true)
#  |-- StockCode: string (nullable = true)
#  |-- Description: string (nullable = true)
#  |-- Quantity: integer (nullable = true)
#  |-- InvoiceDate: string (nullable = true)
#  |-- UnitPrice: double (nullable = true)
#  |-- CustomerID: integer (nullable = true)
#  |-- Country: string (nullable = true)




group_by = source_data.groupBy("Country","InvoiceNo")


aggregate = group_by.agg(f.sum("Quantity").alias("TotalQuantity"),
                         f.max("Quantity").alias("maxQuantity"),
                         f.min("Quantity").alias("minQuantity"),
                         f.avg("Quantity").alias("avgQuantity"),
                         f.sum(f.expr("Quantity  * UnitPrice").alias("Qty_UniPrice"))
                         )

filter = aggregate.where("TotalQuantity > 110")

filter.show()