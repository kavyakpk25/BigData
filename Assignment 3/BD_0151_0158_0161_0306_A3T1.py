#!/usr/bin/python3
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def avg_reduce_func(value1, value2):
    return ((value1[0] + value2[0], value1[1] + value2[1])) 


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("INVALID INPUT")
        sys.exit(-1)
      
# Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("Average Strokes")\
        .getOrCreate()
      
    df = spark.read.csv(sys.argv[3],header=True)
    
    
    word=sys.argv[1]
    lines = df.rdd.map(lambda r:(r["word"], r["recognized"],r["Total_Strokes"]))
    
    valid = lines.filter(lambda x:  word == x[0])
    final = valid.map(lambda r: (r[1], (int(r[2]), 1))).reduceByKey(avg_reduce_func).mapValues(lambda x: x[0]/x[1]) 
    
    length = len(final.collect())
    if(length == 2):
         if(final.collect()[0][0] == "True"):
              print("%.5f" %final.collect()[0][1])
              print("%.5f" %final.collect()[1][1])
         else:
              print("%.5f" %final.collect()[1][1])
              print("%.5f" %final.collect()[0][1])
    elif(length == 1):
        if(final.collect()[0][0] == "True"):
              print("%.5f" %final.collect()[0][1])
              print("%.5f" %0)
        else:
              print("%.5f" %0)
              print("%.5f" %final.collect()[0][1])
    else:
        print("%.5f" %0)
        print("%.5f" %0)
                        
                        
                        
              
          
    
    	        
	       
	       
	       
	       
	       
	       
	       
	

