from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import unix_timestamp,udf,col,monotonically_increasing_id
from pyspark.sql.types import StringType

conf = SparkConf()
conf.setAppName("Spark df excercise")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


list = [
    (1,"2013-07-27",11599,'CLOSED'),
    (2,"2013-07-26",1599,'CLOSED'),
    (3,"2013-07-25",10599,'CLOSED'),
    (4,"2013-07-25",5990,'CLOSED'),
    (5,"2013-07-29",1239,'CLOSED'),
    (6,"2013-07-23",1099,'CLOSED'),
]

schema = ["id","date","customer_id","status"]

df = spark.createDataFrame(list,schema)

# def epoch_time(date):
#     return unix_timestamp(date)


# udf register
# date_epoch_converter = udf(epoch_time)

# create epoch time
unix_time_stamp = df.withColumn("epoch_time",unix_timestamp(col("date"),'yyyy-MM-dd'))

# new_col_df = df.select("id","date","customer_id","status",unix_timestamp(col("date"),'yyyy-MM-dd').alias("unix_time"))


# creating unique id
new_col = unix_time_stamp.withColumn("unique_id",monotonically_increasing_id())

# dropting  duplicates from the columns
drop_Duplicates = new_col.dropDuplicates(["date"])

# drop the column
drop_col = drop_Duplicates.drop("id")

# sort based on column date
sort_col = drop_col.sort("date")
sort_col.show()