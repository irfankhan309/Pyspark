from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("grouping obj expr")
conf.setMaster("local[5]")



spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_data = spark.read.format("csv").option("inferSchema",True).option("header",True)\
             .option("path","D:/pyspark_codes/order_data.csv").load()


groupby = source_data.groupBy("Country","InvoiceNo")

aggregate = groupby.agg(f.sum("Quantity"),
                        f.sum(f.expr("UnitPrice * Quantity")),
                        )


aggregate.show()