from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark confs
conf = SparkConf()
conf.setAppName("spark file read modes")
conf.setMaster("local[5]")

# spark session creation 
spark = SparkSession.builder.config(conf = conf).getOrCreate()


# we have 3 file modes while reading the file 
# if we have mallfarmed data in file then these modes can help while reading the file
#thos meodes are follows
# 1. PERMISSIVE : ITS DEFULT ONE NO NEED TO DEFINE IN THE MODE OF DF 
# 2. DROPMALLFORMED : IT WIILL IGNORE THE MALLFORMED RECORD IN FILE THAT IS IT WILL DROP THE MALLFORMED DATA
# 3. FAILFAST : IT WILL RAISE AN EXCEPTION IF ANY MALLFORMED DATA IS ENCROUTED IN FILE

orders_schema = 'order_id integer,order_date timestamp,order_customer_id integer,order_status string'


# permissive mode for reading the file
# orders_df = spark.read.format("csv").schema(orders_schema).option("header",True)\
#             .option("mode","permissive")\
#             .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()


# orders_df.show()
# orders_df.printSchema()




# dropmalfarmed mode for reading the file & it will  drop the corrupted data in file
# orders_df = spark.read.format("csv").schema(orders_schema).option("header",True)\
#             .option("mode","dropmalformed")\
#             .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()


# orders_df.show()
# orders_df.printSchema()


# fialfast mode for reading , it will raise an exception if courrupted data encountered in file
orders_df = spark.read.format("csv").schema(orders_schema).option("header",True)\
            .option("mode","failfast")\
            .option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv").load()



