# first create a list 
# create header
# create spark config
# create sparksession
# create dataframe from above data and schema
# create a new column for epoch time stamp
# create a newId for uniqid 
# drop duplicates from data based on two columns
# drop the column order_id
# sort the data based on date


from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col,unix_timestamp,monotonically_increasing_id

conf = SparkConf()
conf.setAppName("df ex2")
conf.setMaster("local[5]")



spark = SparkSession.builder.config(conf = conf).getOrCreate()


data = [
    (1,"2013-07-29",11599,"CLOSED"),
    (2,"2013-07-28",1123,"PENDING_PAYMENT"),
    (3,"2013-07-26",119,"PROCESSING"),
    (4,"2013-07-27",1299,"CLOSED"),
    (5,"2013-07-22",1009,"COMPLETED"),
    (6,"2013-07-23",199,"PENDING_PAYMENT"),
    (7,"2013-07-23",1234,"PROCESSING"),
    (8,"2013-07-23",1234,"COMPLETED")

]

schema = ["order_id","order_date","customer_id","status"]

df = spark.createDataFrame(data,schema)


# create a new column for epoch timestamp
epoch = df.withColumn("epochTime",unix_timestamp(col("order_date"),"yyyy-MM-dd"))



# create a new uniq id
uniqId = epoch.withColumn("uniqID",monotonically_increasing_id())

# drop duplicates based on date,customerid
dropDuplicate = uniqId.dropDuplicates(["order_date","customer_id"])

# sorting 
sorted = dropDuplicate.sort("order_date",ascending=False)

sorted.show()