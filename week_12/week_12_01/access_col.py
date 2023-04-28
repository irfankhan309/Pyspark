from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col,column,sum,avg,max



data_path = "D:/pyspark_codes/housing.csv"

# read the data set from the readapi 
# select the columns
# /store it into new dataset with above columns



conf = SparkConf()
conf.setAppName("houseing app")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


source_data = spark.read.format("csv")\
              .option("inferSchema",True)\
              .option("header",True)\
              .option("path",data_path).load()


id = source_data.withColumnRenamed("_c0","id")

# column string 
select_cols = id.select("MedInc","HouseAge","AveRooms","AveBedrms","Population","AveOccup")

# column object string

age = id.where(col("HouseAge") < 20)

# age.show()
select_cols = age.groupBy("HouseAge").agg(sum("Population").alias("sum_population")\
                                         ,avg("Population").alias("avg_population"),max("Population").alias("max_population"))


select_cols.show()

# id.show()
# id.printSchema()
