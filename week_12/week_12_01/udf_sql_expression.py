from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import expr
conf = SparkConf()
conf.setAppName("udf with sqll")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_Data = spark.read.format("csv").option("inferSchema",True)\
       .option("header",True).option("path","D:/pyspark_codes/data1.dataset1").load()


cols =["name","age","location"]

df1 = source_Data.toDF(*cols)

# udf
def age_check(age):
    if age > 18:
        return True
    else:
        return False


# register
spark.udf.register("adult_check",age_check,BooleanType())


# create new column
df2 = df1.withColumn("adult",expr("adult_check(age)"))

df2.show()