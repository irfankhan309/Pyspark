from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import regexp_extract

doc  = """ 
       first read the data file using read.text(path) 
       if the data is inconsistent i.e not in good format and corrupted then aply the regex pattern
       steps are:
       1.load the data file
       2. create regex pattern string
       3. import the regexp_extract funciton from spark.sql.functions
       4. use the select method in df and apply the regex in it.
       """




conf = SparkConf().setAppName("spark regex example").setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

orders_df = spark.read.text("D:/pyspark_codes/pilot_prepare/week_12/orders_new.csv")

regex_pattern = r"^(\S+) (\S+)\t(\S+)\,(\S+)"

result = orders_df.select(
            regexp_extract("value",regex_pattern,1).alias("order_id"),
            regexp_extract("value",regex_pattern,2).alias("order_date"),
            regexp_extract("value",regex_pattern,3).alias("order_customer_id"),
            regexp_extract("value",regex_pattern,4).alias("order_status")              
)


result.groupBy("order_status").count().show()