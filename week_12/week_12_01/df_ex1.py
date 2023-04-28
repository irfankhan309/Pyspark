# first create a list
# create a schema for iit
# create a dataframe from above both schema and list
# convert order_data to epoch timestamp
# create a new column with newID,, uniqueID 
# drop duplicates from above df
# drop the orderid column
# sort it based on orderDate

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import unix_timestamp,col,monotonically_increasing_id

conf = SparkConf()
conf.setAppName("local dataset ex on df")
conf.setMaster("local[4]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


data_list = [(4,"2013-07-27",1590,"PROCESSING"),
    (5,"2013-07-28",2343,"COMPLETED"),
    (6,"2013-07-28",2343,"COMPLETED"),
    (1,"2013-07-25",11599,"CLOSED"),
    (2,"2013-07-25",12599,"PENDINT_PAYMENT"),
    (3,"2013-07-26",1011,"PENDING")
]

schema = ["order_id","order_date","customer_id","order_status"]

df1 = spark.createDataFrame(data_list,schema)

# CREATE EPOCH TIMESTAMP
epoch_time = df1.withColumn("date1",unix_timestamp(col("order_date"),"yyyy-MM-dd"))

# CREEAE UNIQID
uniqID = epoch_time.withColumn("unique_id",monotonically_increasing_id())


# DROP THE DUPLICATE BASED ON ORDER_DATA,CUSTOMERID
drop_duplicate = uniqID.dropDuplicates(["order_date","customer_id"])

# drop column i.e order_id from the dataframe
drop_col = drop_duplicate.drop("order_id")


# sort based on order_date
sorted = drop_col.sort(col("order_date"))


sorted.show()