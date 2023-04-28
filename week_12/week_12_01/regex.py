from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

conf = SparkConf()
conf.setAppName("handling unstructured data using regex")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


# my_regex = "^(\S+) (\S+)\t(\S+)\,(\S+)"
myregex = r'^(\S+) (\S+)    (\S+)\,(\S+)'

# source_data = spark.read.format("csv").option("inferSchema",True)\
#             .option("path","D:/pyspark_codes/pilot_prepare/week_12/week_12_01/new_orders.csv")\
#             .load()

source_data = spark.read.text("D:/pyspark_codes/pilot_prepare/week_12/week_12_01/new_orders.csv")

# cleaned_data = source_data.select(F.regexp_extract("value",myregex,1).alias("order_id"),
#                                   F.regexp_extract("value",myregex,2).alias("order_date"),
#                                   F.regexp_extract("value",myregex,3).alias("order_customer_id"),
#                                   F.regexp_extract("value",myregex,4).alias("order_status")
#                                   )

final_df =source_data.select(F.regexp_extract('value',myregex,1).alias("order_id"),
                             F.regexp_extract('value',myregex,2).alias("date"),
                             F.regexp_extract('value',myregex,3).alias("customer_id"),
                             F.regexp_extract('value',myregex,4).alias("status"))

final_df.show()



source_data.show()
# source_data.show()
# source_data.printSchema()