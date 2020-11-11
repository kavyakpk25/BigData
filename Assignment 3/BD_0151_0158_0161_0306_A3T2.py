#!/usr/bin/python3
from __future__ import print_function

import re
import sys
from operator import add

#0 file 1 word 2 strokes 3 data1 4 data2

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("INVALID INPUT")
        sys.exit(-1)
      
# Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("Object count")\
        .getOrCreate()
      
    df_stat = spark.read.csv(sys.argv[4],header=True)
    df_shape = spark.read.csv(sys.argv[3],header=True)
    
    word=sys.argv[1]
    strokes = int(sys.argv[2])
    
    df_shape = df_shape.repartition('countrycode')
          
    joined1 = df_stat.join(df_shape, df_stat.key_id == df_shape.key_id, "inner").drop(df_shape.key_id).drop(df_shape.word).drop(df_stat.timestamp)
    
    
    joined = joined1.rdd.map(lambda r:(r["countrycode"], r["word"], r["recognized"], r["key_id"], r["Total_Strokes"]))
    joined = joined.filter(lambda x: word == x[1]).filter(lambda x: "False" == x[2]).filter(lambda x: int(x[4]) < strokes)  
    joined = joined.map(lambda r: (r[0],1)).reduceByKey(add).sortByKey(ascending=True)
    
    if(len(joined.collect()) == 0):
        print(0)
    else:
        for x in joined.collect():
            print(x[0] + "," + str(x[1])) 
           

	       
 
	       
	       
	       
	

