from pyspark.sql import SparkSession 
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType,IntegerType



# spark configuration
conf = SparkConf()
conf.setAppName("UDF OBJECT EXPRESSION")
conf.setMaster("local[5]")


# spark session creation
spark = SparkSession.builder.config(conf = conf).getOrCreate()

# read csv file AND THIS FILE DOES NOT CONTAIN HEADER
source_data = spark.read.format("csv").option("inferSchema",True).option("header",True)\
                .option("path","D:/pyspark_codes/data1.dataset1").load()


# create schema and attach to above dataframe
cols = ["name","age","location"]

df1  =source_data.toDF(*cols)

# creating func
def adult_check(age):
    if age > 18:
        return True
    else:
        return False

# lambda function fr udf another approach
len1 = lambda x: len(x)

col_len_udf = udf(len1,IntegerType())


# UDF object creation
adult_check = udf(adult_check,BooleanType())

# new column creation 
df2 = df1.withColumn("adult",adult_check("age"))

# select col
df3 = df2.select(col_len_udf("name").alias("len_name"),col_len_udf("location").alias("len_location"),adult_check("age"))

df3.write.format("csv").option("mode","overwrite").option("path","D:/pyspark_codes/pilot_prepare/week_12/week_12_01/udf_data/").save()


df3.show()