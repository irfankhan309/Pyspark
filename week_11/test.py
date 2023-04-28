from pyspark.sql import SparkSession

from pyspark import SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql.functions import col,to_date,date_format,current_timestamp,explode

conf = SparkConf()
conf.setAppName("test-data")
conf.setMaster("local[4]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

# data2 = [("James","","William","36636","M",3000, "01/01/1991",['java','python']),
 
#         ("Michael","Smith","","40288","M",4000,"05/01/1990",['R','python']),
 
#         ("Robert","","Dawson","42114","M",4000, "02/09/1994",['java','Scala']),
 
#         ("Maria","Jones","","39192","F",4000, "02/09/1994",['Scala','R'])
 
#   ]

# data2 = [("James","","William","36636","M",3000, "1991/21/12",['java','python']),
 
#         ("Michael","Smith","","40288","M",4000,"1990/25/11",['R','python']),
 
#         ("Robert","","Dawson","42114","M",4000, "1994/29/12",['java','Scala']),
 
#         ("Maria","Jones","","39192","F",4000, "1994/18/10",['Scala','R'])
 
#   ]

data2 = [("James","","William","36636","M",3000, "12/1991/15",['java','python']),
 
        ("Michael","Smith","","40288","M",4000,"11/1990/16",['R','python']),
 
        ("Robert","","Dawson","42114","M",4000, "10/1994/17",['java','Scala']),
 
        ("Maria","Jones","","39192","F",4000, "12/1994/18",['Scala','R'])
 
  ]



schema = ["firstname" , "middlename" , "lastname" , "id", "Gender", "Salary", "dob", "Subjects"]



df1 = spark.createDataFrame(data = data2 ,schema = schema)


int_id = df1.withColumn('id',df1["id"].cast(IntegerType()))

# date1 = int_id.withColumn("dob",to_date(int_id.dob,"MM/yyyy/dd"))

date1 = int_id.withColumn("dob",to_date(int_id.dob,"MM/yyyy/dd"))

li1 = date1.select(date1.firstname,\
                   date1.middlename,\
                   date1.lastname,\
                  date1.id,\
                  date1.Gender,\
                  date1.Salary,\
                  date1.dob,\
                  date1.Subjects[0].alias("subject1"),date1.Subjects[1].alias("subject2"))


li1.show()

li1.printSchema()


# create spark session - done

# readdata - done

# method for transormations id 

# date format yyyy-mm-dd

# two seperate columns
