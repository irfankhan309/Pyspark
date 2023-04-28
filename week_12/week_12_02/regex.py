from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import regexp_extract

conf = SparkConf().setAppName("using regex to clean the data file")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

# order_df  = spark.read.format("csv").option("inferSchema",True).option("header",True)\
#             .load("D:/pyspark_codes/pilot_prepare/week_12/orders_new.csv")


orders_df = spark.read.text("D:/pyspark_codes/pilot_prepare/week_12/orders_new.csv")

# orders_df.show(truncate =False)
# data ouptul
# |value                              |
# +-----------------------------------+
# |1 2013-07-25\t11599,CLOSED         |
# |2 2013-07-25\t256,PENDING_PAYMENT  |
# |3 2013-07-25\t12111,COMPLETE

# reg_pattern = r"^(\S+) (\S+)    (\S+)\,(\S+)"
reg_pattern = r"^(\S+) (\S+)\t(\S+)\,(\S+)"

result_df = orders_df.select(
                regexp_extract('value',reg_pattern,1).alias("order_id"),
                regexp_extract("value",reg_pattern,2).alias("order_Date"),
                regexp_extract('value',reg_pattern,3).alias("order_customer_id"),
                regexp_extract('value',reg_pattern,4).alias("order_status")    
            )

result_df.show()