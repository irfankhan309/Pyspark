from pyspark.sql import SparkSession
from pyspark import SparkConf


conf = SparkConf()
conf.setAppName("spark join")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

orders_df = spark.read.format("csv")\
            .option("inferSchema",True).option("header",True).option("mode","dropmalformed")\
            .load("D:/pyspark_codes/pilot_prepare/week_12/week_12_02/datasets/orders.csv")


customer_df  = spark.read.format("csv").option("inferSchema",True).option("header",True)\
                .option("mode","dropmalformed")\
                .load("D:/pyspark_codes/pilot_prepare/week_12/week_12_02/datasets/customers.csv")

# join condtion
join_condition = orders_df.order_customer_id == customer_df.customer_id

# join type
# join_type = 'inner'
join_type = 'left'

# join df
join_df = orders_df.join(customer_df, join_condition,join_type)

join_df.show()