# spark configs
# spark sessions
# spark df reader api for data load
# crete schema 
# attahce scheam to df
# creae udf for adult check
# register udf in spark sql
# apply this udf for the column using with column

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf,expr

conf = SparkConf()
conf.setAppName("spark udf using sql expression")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv").option("inferSchema",True).option("header",True)\
             .option("path","D:/pyspark_codes/data1.dataset1").load()


schema = ["name","age","location"]

df  = source_data.toDF(*schema)

# udf
def adult(age):
    if age > 18:
        return True
    else:
        return False


# now registering the udf in spark sql
spark.udf.register("adult_checker",adult,BooleanType())

# now use above udf in column creation based on age condition
adult_df = df.withColumn("adult_or_not",expr("adult_checker(age)"))

# to list the udf functions
for x in spark.catalog.listFunctions():
    if x.name  == 'adult_checker':
        print(x)

adult_df.show()