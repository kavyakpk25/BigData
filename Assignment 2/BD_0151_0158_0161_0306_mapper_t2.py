#!/usr/bin/python3
import sys

page_rank={}
path=sys.argv[1]
f = open(path, "r")

line=f.readline()
while (line):
    
    line.strip()
    node,pr=line.split(",")
    node=node.strip()
    
    page_rank[node]=float(pr)
    line=f.readline()

#mapper reads the adj_lst file and returns key value
for inp in sys.stdin: 
    if inp:
    	inp.strip()
    	#l=[]
    	node,adj_list=inp.split("\t")
    	node = node.strip()
    	#l.append(page_rank[node])
    	adj_list =adj_list[2:-2]
    	adj_list =adj_list.split(',')
    	adj_list=[i.strip() for i in adj_list]
    	a = "\'"
    	adj_list=[i.replace(a, "") for i in adj_list]
    	
    	print(node,"\t",0)
    	for i in adj_list:
    		print(i,"\t",page_rank[node]/len(adj_list))
    	#print(node,"\t",page_rank[node]/len(adj_list))
f.close()	
