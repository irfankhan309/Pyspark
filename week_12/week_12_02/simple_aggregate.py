from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import  functions as F


conf = SparkConf()
conf.setAppName("spark simple aggregations")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


orders_df = spark.read.format("csv").option("header",True).option("inferScehma",True)\
            .load("D:/pyspark_codes/pilot_prepare/week_12/order_data.csv")


# schema
# InvoiceNo|StockCode|Description|Quantity|InvoiceDate|UnitPrice|CustomerID|Country|
simple_agg  = orders_df.select(F.count("*").alias("rowscount"),
                               F.sum("UnitPrice").alias("priceOfUnit"),
                               F.max("Quantity").alias("max_quantity"),
                               F.sum("Quantity").alias("total_quantity"),
                               F.countDistinct("InvoiceNo").alias("original_invoice_no")
                               )


simple_agg.show()