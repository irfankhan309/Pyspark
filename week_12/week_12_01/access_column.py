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

# cls = HouseAge	AveRooms	AveBedrms	Population	AveOccup	Latitude


column = id.select(column("HouseAge"),column("AveRooms"),column("AveBedrms"),column("Population"),column("AveOccup"))


column.show()