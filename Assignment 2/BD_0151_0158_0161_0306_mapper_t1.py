#!/usr/bin/python3
import sys

for input1 in sys.stdin:
    try:
    	if input1[0]!='#':
        	from_node,to_node=input1.split() 
        	print(from_node+"\t"+to_node)
    except:
        continue
