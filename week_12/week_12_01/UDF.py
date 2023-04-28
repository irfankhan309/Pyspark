from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

conf = SparkConf()
conf.setAppName("UDF CUSTOMER")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

source_Data = spark.read.format("csv").option("inferSchema",True).option("header",True)\
                 .option("path","D:/pyspark_codes/data1.dataset1").load()



cols = ["name","age","city"]

conv = source_Data.toDF(*cols)

# create udf 
def age_check(age):
    if age > 18:
        return True
    else:
        return False


# now  create UDF 
parse_age_udf = udf(age_check,BooleanType())

# now using UDF in our transformations to create new a column
age = conv.withColumn("adult",parse_age_udf("age"))




age.show()
age.printSchema()