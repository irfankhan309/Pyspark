from pyspark.sql import SparkSession
from pyspark import SparkConf


conf = SparkConf()
conf.setAppName("spark-regex-app")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

data = [['id_20_30', 10], ['id_40_50', 30]]

schema  = ["id","age"]


df = spark.createDataFrame(data,schema)

df.show()