from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col,concat,expr

conf = SparkConf()
conf.setAppName("spark refer a column using object expresion")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

orders_df = spark.read.format("csv").option("inferSceham",True)\
           .option("header",True).option("path","D:/pyspark_codes/pilot_prepare/week_12/orders.csv")\
           .load()



cols = orders_df.select(col("order_date").alias("DATE"),
                        col("order_customer_id").alias("customer_id"),
                        col("order_status").alias("status")
                        )


add_expression  = cols.select(col("DATE"), col("customer_id"),expr("status || '_STATUS' ")    
                              )

add_expression.show()