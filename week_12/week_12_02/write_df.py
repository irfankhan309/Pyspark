from pyspark.sql import SparkSession
from pyspark import SparkConf


conf = SparkConf()
conf.setAppName("spark write df")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

orders_df = spark.read.format("csv").option("header",True)\
            .option("inferSchema",True)\
            .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv")\
            .load()

# print("========================================")
# print(orders_df.head(12))
# print("========================================")
# print(orders_df.take(10))
# print("========================================")

# overwrite mode for file writeing
# write_df = orders_df.write.format("csv").mode("overwrite")\
#            .option("path",'D:/pyspark_codes/pilot_prepare/week_12/week_12_02/results').save()


# append mode for file writing
# write_df = orders_df.write.format("csv").mode("append")\
#            .option("path",'D:/pyspark_codes/pilot_prepare/week_12/week_12_02/results').save()

# errorifExists mode for file writeing
# write_df = orders_df.write.format("csv").mode("errorifexists")\
#            .option("path",'D:/pyspark_codes/pilot_prepare/week_12/week_12_02/results').save()


# ignore mode for file writeing
write_df = orders_df.write.format("csv").mode("ignore")\
           .option("path",'D:/pyspark_codes/pilot_prepare/week_12/week_12_02/results').save()




