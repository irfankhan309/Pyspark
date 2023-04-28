from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,column,to_date,date_format,current_timestamp,explode


conf = SparkConf()
conf.setAppName("hitachi test")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()


data2 = [("James","","William","36636","M",3000, "12/1991/15",['java','python']),
 
        ("Michael","Smith","","40288","M",4000,"11/1990/16",['R','python']),
 
        ("Robert","","Dawson","42114","M",4000, "10/1994/17",['java','Scala']),
 
        ("Maria","Jones","","39192","F",4000, "12/1994/18",['Scala','R'])
 
  ]


       




schema = ["firstname" , "middlename" , "lastname" , "id", "Gender", "Salary", "dob", "Subjects"]

# rdd1 = spark.sparkContext.parallelize(data2)

# df  = rdd1.toDF(schema)

# print(rdd1.take(10))

# df.show()

df = spark.createDataFrame(data2, schema)

conv = df.withColumn("id",df.id.cast(IntegerType()))


conv2 = conv.withColumn("new_id",conv.id*2)
conv3 = conv2.withColumn("salary_tax",conv2.Salary*.20)
conv4 = conv3.withColumn("net_salary",conv3.Salary-conv3.salary_tax)

date = conv4.withColumn("dob",to_date(conv4.dob,"MM/yyyy/dd").alias("DOB"))


subjects = date.select(
    date.firstname,date.middlename,date.lastname,date.id,date.Gender,date.Salary,date.dob,date.Subjects[0],
    date.Subjects[1]
)






subjects.show()
subjects.printSchema()