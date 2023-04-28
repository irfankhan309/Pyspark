from pyspark.sql import SparkSession
from pyspark import SparkConf



conf = SparkConf()
conf.setAppName("join dfs")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

emp_df = spark.createDataFrame(emp,empColumns)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]


dept_df = spark.createDataFrame(dept,deptColumns)

print("INNER JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
joindf = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "inner").show()
print("INNER JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

print("left JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
leftJoin = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id,"left").show()
print("left JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


print("right  JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
rightJOin = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id,"right").show()
print("right  JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

print("full outer  JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
outerjoin = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "fullouter").show()
print("full outer  JOIN <<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

# joindf.show()