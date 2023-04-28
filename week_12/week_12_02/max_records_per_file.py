from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("spark max records per file ")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

orders_df = spark.read.format("csv").option("header",True).option("inferScehma",True)\
            .option("path",'D:/pyspark_codes/pilot_prepare/week_12/orders.csv').load()


# with out transformations below we are write the data to file with maxRecordsPerFile to set the num of records in file
writer_df = orders_df.write.format("csv")\
            .mode("overwrite").option("maxRecordsPerFile",5000)\
            .option("path","D:/pyspark_codes/pilot_prepare/week_12/week_12_02/max_records_perFile")\
            .save()



