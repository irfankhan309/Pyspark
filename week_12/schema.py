from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,ArrayType

from pyspark.sql import types as t

# print(dir(t))


conf = SparkConf()
conf.setAppName("explicit schema")
conf.setMaster("local[4]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()


data2 = [("James","","William","36636","M",3000, "12/1991/15",['java','python']),
 
        ("Michael","Smith","","40288","M",4000,"11/1990/16",['R','python']),
 
        ("Robert","","Dawson","42114","M",4000, "10/1994/17",['java','Scala']),
 
        ("Maria","Jones","","39192","F",4000, "12/1994/18",['Scala','R'])
 
  ]

schema = ["firstname" , "middlename" , "lastname" , "id", "Gender", "Salary", "dob", "Subjects"]

emp_schema = StructType([
    StructField("firstname",StringType()),
    StructField("middlename",StringType()),
    StructField("lastname",StringType()),
    StructField("id",StringType()),
    StructField("Gender",StringType()),
    StructField("salary",IntegerType()),
    StructField("DOB",StringType()),
    StructField("list",ArrayType(StringType()),True)
])


# df  = spark.createDataFrame(data2,schema)

# df.show()
# df.printSchema()


