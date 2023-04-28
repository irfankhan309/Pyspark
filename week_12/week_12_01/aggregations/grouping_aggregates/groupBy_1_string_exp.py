from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f


conf = SparkConf()
conf.setAppName("groupe by usgin string expr")
conf.setMaster("local[5]")



spark = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv").option("header",True).option("inferSchema",True)\
             .option("path","D:/pyspark_codes/order_data.csv").load()



groupby = source_data.groupBy("Quantity","InvoiceNo")


aggregate = groupby.agg(
    f.expr("sum(UnitPrice)"),
    f.expr("sum(Quantity)"),
    f.expr("sum(UnitPrice * Quantity) as TotalUnitPrice")
).where("TotalUnitPrice > 30 ")

select = aggregate.where("Quantity > 10").select("InvoiceNo")


select.show()