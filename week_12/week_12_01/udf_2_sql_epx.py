from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf,expr
from pyspark.sql.types import BooleanType

conf = SparkConf()
conf.setAppName("udf sql expr")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_data = spark.read.format("csv").option("inferSchema",True)\
               .option("header",True)\
               .option("path","D:/pyspark_codes/data1.dataset1").load()

# create a schema
cols = ["name","age","location"]

df = source_data.toDF(*cols)

def adult_check(age):
    if age > 18:
        return True
    else:
        return False
    
# now create udf register
spark.udf.register("age_check",adult_check,BooleanType())

# now apply above udf in df for new column creation 
age = df.withColumn("adult",expr("age_check(age)"))

age.show()