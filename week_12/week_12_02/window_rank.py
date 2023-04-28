from pyspark.sql import SparkSession,Window 
from pyspark import SparkConf
from pyspark.sql.functions import row_number,rank,dense_rank,desc


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), 
    ("Michael", "Sales", 4600),  
    ("Robert", "Sales", 4100),  
    ("Maria", "Finance", 3000),  
    ("James", "Sales", 3000),    
    ("Scott", "Finance", 3000),  
    ("Jen", "Finance", 3900),    
    ("Jeff", "Marketing", 3000), 
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100) 
  )

columns = ["employee_name", "department", "salary"]

df = spark.createDataFrame(simpleData,columns)


window = Window.partitionBy("department").orderBy("salary")

window_fun = df.withColumn("rank",rank().over(window))

window_fun.show()