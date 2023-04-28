from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("spark join")
conf.setMaster("local[5]")


spark = SparkSession.builder.config(conf = conf).getOrCreate()

# emp data and creation of dataframe
emp_data = [(1,"Smith",-1,"2018","10","M",3000), \
        (2,"Rose",1,"2010","20","M",4000), \
        (3,"Williams",1,"2010","10","M",1000), \
        (4,"Jones",2,"2005","10","F",2000), \
        (5,"Brown",2,"2010","40","",-1), \
        (6,"Brown",2,"2010","50","",-1) \
        ]

empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]


df1 = spark.createDataFrame(emp_data,empColumns)


# create dept data & dataframe
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]

df2 = spark.createDataFrame(dept,deptColumns)

join_condition = df1.emp_dept_id == df2.dept_id

join_type = 'inner'
# join
join_df  = df1.join(df2,join_condition,join_type)

join_df.show()