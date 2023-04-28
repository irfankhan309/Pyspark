# create spark session with hive enable support to store the data in table
# load/read the datafile which is a paritioned directory 
# write that entire data to a directory

from pyspark.sql import SparkSession
from pyspark import SparkConf


conf = SparkConf()
conf.setAppName("sparks sql transform")
conf.setMaster("local[5]")

# create a SPARK SESSION
spark = SparkSession.builder.config(conf = conf).getOrCreate()

directory_path = "D:/pyspark_codes/pilot_prepare/week_12/orders_data/"


# read the source data from direcotry where it has subdirectories and files
source_data = spark.read.format("csv")\
            .option("recursiveFileLookup","true")\
            .option("inferSchema",True).option("header",True)\
            .option("mode","DROPMALLFARMED").option("path",directory_path)\
            .load()

            
# writing the above df whether all data read and loaded or not for that storeing below to a another dir.
source_data.write.format("csv").option("mode","overwrite").option("path","D:/pyspark_codes/pilot_prepare/week_12/all_data/").save()

source_data.show()
source_data.printSchema()