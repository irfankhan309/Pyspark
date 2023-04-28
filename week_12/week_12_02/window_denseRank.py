from pyspark.sql import SparkSession,Window
from pyspark import SparkConf
from pyspark.sql.functions import dense_rank,rank,row_number,desc

# spark conf
conf = SparkConf()
conf.setAppName("spark window function denseRank")
conf.setMaster("local[50]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()




data = [
    ("James", "Sales", 3000), 
    ("Michael", "Sales", 4600),  
    ("Robert", "Sales", 4100),  
    ("Maria", "Finance", 3000),  
    ("James", "Sales", 3000),    
    ("Scott", "Finance", 3000),  
    ("Jen", "Finance", 3900),    
    ("Jeff", "Marketing", 3000), 
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100) 
]

schema  = ["name","department","salary"]

# create 
df = spark.createDataFrame(data,schema)

# window
mywindow = Window.partitionBy("department").orderBy("salary")

window = df.withColumn("denseRank",dense_rank().over(mywindow))

window.show()