from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col


conf = SparkConf()
conf.setAppName("spark job constistency award")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


data = [(1,'A','Phy','90'),
        (1,'A','Che','95'),
        (2,'B','Phy','80'),
        (2,'B','Che','85'),
        (3,'C','Phy','90'),
        (4,'D','Phy','75'),
        (4,'D','Che','90'),
]
schema  = ["STUDID","NAME","SUBJECT","MARKS"]


df1 = spark.createDataFrame(data, schema=schema)

join_condition = df1.STUDID == df1.STUDID


join_type = 'inner'

join_df = df1.join(df1,join_condition,join_type)

join_df.show()