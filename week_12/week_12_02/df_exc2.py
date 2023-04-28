""" 1.create sparksession
    2. create a list
    3. create schema
    4.create a dataframe on top of it
    5. add new_column i.e epoch time based the date
    6. create a new id whcih unique id
    7. drop duplicates based on duplicated repeated column
    8. drop col which is order_id
    9. sort based on date

"""
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import unix_timestamp,udf,col,monotonically_increasing_id

conf = SparkConf()
conf.setAppName("spark df ex")
conf.setMaster("local[5]")

# spark session creation
spark = SparkSession.builder.config(conf = conf).getOrCreate()



data = [
    (1,"2013-07-27",11599,'CLOSED'),
    (2,"2013-07-26",1599,'CLOSED'),
    (3,"2013-07-25",10599,'CLOSED'),
    (4,"2013-07-25",5990,'CLOSED'),
    (5,"2013-07-29",1239,'CLOSED'),
    (6,"2013-07-23",1099,'CLOSED'),
]

schema = ["id","date","customer_id","status"]

# data frame creation
df1 = spark.createDataFrame(data,schema)


# create new col with epoch time on existing column date 
epochtime = df1.withColumn("epoch_time",unix_timestamp(col("date"),'yyyy-MM-dd'))

uniqID  = epochtime.withColumn("unique_id",monotonically_increasing_id())

# droping duplicates from the data
drop_duplicates = uniqID.dropDuplicates(['date'])

dropColumn = drop_duplicates.drop("id")


dropColumn.show()