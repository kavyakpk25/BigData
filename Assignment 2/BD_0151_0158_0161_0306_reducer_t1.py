#!/usr/bin/python3
from operator import itemgetter
import sys

current_word = None
current_node_list=[]
word = None
v_file_path=sys.argv[1]
from_list=set()
to_list=set()
file1=open(v_file_path,"w")
for line in sys.stdin:
    line = line.strip()
    from_node, to_node = line.split('\t', 1)
    
    from_list.add(from_node)
    to_list.add(to_node)
    
    if current_word == from_node:
    	if(to_node not in current_node_list):
            current_node_list.append(to_node)  	
    else:
        if(current_word):
            
            print(current_word,"\t",current_node_list)
            file1.write(str(current_word)+","+str(1)+"\n")
            current_node_list=[]
            current_node_list.append(to_node)      
        else:
            current_node_list.append(to_node) 		
    current_word = from_node
    
if current_word == from_node:
   
    print(current_word,"\t",current_node_list)
    file1.write(str(current_word)+","+str(1)+"\n")

for i in to_list.difference(from_list):

	file1.write(str(i)+","+str(0)+"\n")

file1.close()
