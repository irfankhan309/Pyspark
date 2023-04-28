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

# MY FUNCTION
def adult_check(age):
    if age > 18:
        return 'YES'
    else:
        return "NO"

#  REGISTER THE UD WITH ABOVE FUNCITON
adult_check_udf = udf(adult_check,StringType())


new_col = df1.withColumn("adult",adult_check_udf(col("age")))

new_col.show()