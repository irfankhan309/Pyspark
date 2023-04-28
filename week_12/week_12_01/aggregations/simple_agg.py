# simpel aggretations
# load teh file n create a df
# will do some simpel aggregations

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("simple aggregations")
conf.setMaster("local[5]")

# create saprksession
spark = SparkSession.builder.config(conf = conf).getOrCreate()

# lload data n create datafrmae
source_data = spark.read.format("csv").option("inferSchema",True)\
            .option("header",True).option("path","D:/pyspark_codes/order_data.csv")\
            .load()


# aggregations
aggs  = source_data.select(f.count("*"),
                            f.sum(f.col("Quantity")),
                            f.avg("Quantity").alias("avg_qty"),
                            f.countDistinct("InvoiceNo").alias("count_distinct")
                            )

aggs.show()
# source_data.printSchema()