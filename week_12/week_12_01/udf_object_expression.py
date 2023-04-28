# udf using object expression
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf,col
from pyspark.sql.types import BooleanType




conf = SparkConf()
conf.setAppName("UDF WITH OBJECT Expression")
conf.setMaster("local[5]")



spark = SparkSession.builder.config(conf  =conf).getOrCreate()


source_data = spark.read.format("csv").option("inferSchema",True)\
            .option("header",True).option("path","D:/pyspark_codes/data1.dataset1").load()


# schema
cols = ["name","age","location"]

df = source_data.toDF(*cols)


# own functions
def age_check(age):
    if age > 18:
        return True
    else:
        return False
    

# udf register
age_check_udf = udf(age_check,BooleanType())

# create new column with withColumn 
adult_col = df.withColumn("adult",age_check_udf("age"))


adult_col.show()
