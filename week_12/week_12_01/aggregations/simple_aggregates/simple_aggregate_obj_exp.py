from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f

conf = SparkConf()
conf.setAppName("simple aggregate object expr")
conf.setMaster("local[5]")

spark = SparkSession.builder.config(conf = conf).getOrCreate()

# laod the data file from file
source_data = spark.read.format("csv").option("inferSchema",True)\
        .option("header",True).option("path","D:/pyspark_codes/order_data.csv").load()

# |InvoiceNo|StockCode|Description|Quantity|InvoiceDate|UnitPrice|CustomerID|Country

# simple aggregration using object expr
obj_exp = source_data.select(
                f.count("*"),
                f.sum("Quantity").alias("qty"),
                f.avg("Quantity").alias("AVG_QTY"),
                f.max("Quantity").alias("MAX_QTY")
                
            )


# simple aggregate using string expression
str_exp = source_data.selectExpr(
                "count(*) as count",
                "sum(Quantity) as total",
                "avg(Quantity) as avg_qty",
                "max(Quantity) as max_qty",
                "min(Quantity) as min_qty",
            )
# str_exp.show()


# simple aggregate using sql expresssiion
source_data.createOrReplaceTempView("orders")

sql_exp = spark.sql(
                    """ 
                    select count(*) as count,
                    sum(Quantity) as total,
                    MAX(Quantity) as max,
                    MIN(Quantity) as min,
                    avg(Quantity) as avg_qty 
                    FROM ORDERS
                
                    """)

sql_exp.show()

