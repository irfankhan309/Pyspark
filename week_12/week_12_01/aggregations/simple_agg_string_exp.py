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


# simpel aggr with sql/string expr
source_data.selectExpr("count(*) as rowcount",
                       "sum(Quantity) as total_qty",
                       "avg(Quantity) as average_qty",
                       "count(Distinct(InvoiceNo)) as invoices",
                       "max(Quantity) as max_qty"                       
                       ).show()