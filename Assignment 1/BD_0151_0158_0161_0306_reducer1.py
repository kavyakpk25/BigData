#!/usr/bin/python3
import sys

current_word = None
current_count1 = 0
current_count2 = 0
word = None
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    if current_word == word:
        if(count == 1):
            current_count1 += count
        if(count == 2):
            current_count2 += count
    else:
        if current_word:
            if(current_count2 == 0):
               print(current_count1)
            elif(current_count1 == 0):
               current_count2 = current_count2 // 2
               print(current_count2)
            else:
               print(current_count1)
               current_count2 = current_count2 // 2
               print(current_count2)
        if(count == 1):
            current_count1 = 1
            current_count2 = 0
        else:
            current_count2 = 2
            current_count1 = 0
        current_word = word
if current_word == word:
    if(current_count2 == 0):
               print(current_count1)
    elif(current_count1 == 0):
               current_count2 = current_count2 // 2
               print(current_count2)
    else:
               print(current_count1)
               current_count2 = current_count2 // 2
               print(current_count2)
