#!/usr/bin/env python3
import sys

current_node = None
sum = 0.0
node = None
for line in sys.stdin:
	line = line.strip()
	#print(line)
	node, val = line.split("\t")
	val=float(val)
	if current_node == node:
		sum += val
		
	else:
		if current_node:
			newpage_rank = "{0:.5f}".format((sum*0.85) + 0.15)
			print(current_node+","+newpage_rank)
			sum = 0.0
			sum+=val
		else:
			sum+=val
	current_node = node
if current_node == node:
	newpage_rank = "{0:.5f}".format((sum*0.85) + 0.15)
	print(current_node+","+newpage_rank)
