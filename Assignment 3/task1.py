#!bin/python3
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)
      
# Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("Average Strokes")\
        .getOrCreate()
      
    df = spark.read.csv(sys.argv[2],header=True)
    df.printSchema()
    word=sys.argv[1]
    #lines = df.rdd.map(lambda r: (r["word"],r["recognized"],r["Total_Strokes"]))
    lines = df.rdd.map(lambda r:(r["word"],r["recognized"],r["Total_Strokes"])) #if r["word"] == word)
    valid = lines.filter(lambda x:  word == x[0] )
    #lines = valid.map(lambda r:(r[1],r[2])).groupByKey().mapValues(list)
    count= valid.map(lambda r: (r[1],1)).reduceByKey(add)
    average = valid.map(lambda r: (r[1],int(r[2]))).reduceByKey(add)
    #valid2=lines.groupBy("recognized")
    i=0
    for x,y in count.collect(),average.collect():
	     print(x[1]//y[1])
	     i+=1
	     if(i ==10):
	       break
	       
	       
	

