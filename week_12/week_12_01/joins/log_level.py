from pyspark.sql import SparkSession
from pyspark import SparkConf


conf = SparkConf()
conf.setAppName("log level")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv").option("inferShema",True).option("header",True)\
              .option("path","D:/pyspark_codes/bigLog.txt").load()


source_data.createOrReplaceTempView("log_level_tab")

Q1 = spark.sql("select level,date_format(datetime,'MMMM') AS MONTH, count(1) as total from log_level_tab group by level,MONTH")

Q1.show()