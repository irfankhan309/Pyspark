from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

conf = SparkConf()
conf.setAppName("spark udf")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

data_df = spark.read.format("csv").option("inferSchema", True).option("header", True)\
            .load("D:/pyspark_codes/pilot_prepare/week_12/dataset1")


schema = ["name","Age","location"]

df1 = data_df.toDF(*schema)


# myfun
def age_parser(age):
    if age > 18:
        return 'Yes'
    else:
        return 'No'
    
# register udf with colum object expression udf
spark.udf.register("adult_checker", age_parser,StringType())

# create spark sql table
df1.createOrReplaceTempView("data_tab")

query1 = spark.sql("select name,age,location, adult_checker(age) from data_tab")
query1.show()