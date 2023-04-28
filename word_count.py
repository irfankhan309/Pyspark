from pyspark import SparkContext


sc = SparkContext("local[4]","word-count")

source_data = sc.textFile("D:/pyspark_codes/pilot_prepare/search_data.txt")

map_data = source_data.flatMap(lambda x: x.split(" "))

add_value = map_data.map(lambda x: (x.upper(),1))

count = add_value.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0]))

sort = count.sortByKey(False).map(lambda x: (x[1],x[0]))

result = sort.take(50) 
print(result)