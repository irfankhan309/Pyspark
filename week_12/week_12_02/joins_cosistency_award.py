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
schema  = ["studid","NAME","SUBJECT","MARKS"]


df1 = spark.createDataFrame(data, schema=schema)

# join_condition = df1.STUDID == df1.STUDID


join_type = 'inner'

rename_df = df1.withColumnRenamed("studid","new_id").withColumnRenamed("NAME","new_name")\
            .withColumnRenamed("SUBJECT","new_subject").withColumnRenamed("MARKS","new_marks")

join_condition = df1.studid == rename_df.new_id  

join_df = df1.join(rename_df,
            join_condition,      
            join_type
            )

result = join_df.filter((df1.MARKS > 80)  & (rename_df.new_marks >80) &  (df1.SUBJECT != rename_df.new_subject))
 
result.dropDuplicates(['NAME']).show()

