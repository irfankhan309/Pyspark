from pyspark import SparkContext

sc = SparkContext("local[4]","spent analysis")

source = sc.textFile("D:/pyspark_codes/pilot_prepare/customer_orders.csv")

map_data = source.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))

count = map_data.reduceByKey(lambda x,y : x+y)

# result = count.take(40)
# print(result)

print(sc.defaultParallelism)
print(map_data.getNumPartitions())