#!/usr/bin/python3

import sys

current_word = None
current_count = 0
word = None

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t')
    try:
        count = int(count)
    except ValueError:
        continue
    if current_word == word:
        current_count+=count
    else:
        if current_word:
            print(current_word.strip()+","+ str(current_count))
        current_count=1
        current_word = word

if current_word == word:
    print(word.strip()+","+ str(current_count))
