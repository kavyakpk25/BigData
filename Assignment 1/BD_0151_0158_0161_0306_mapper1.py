#!/usr/bin/python3
import sys
import json
import datetime

def clean_record(record):
    word=all(x.isalpha() or x.isspace() for x in record['word'])
    country= len(record['countrycode']) == 2 and record['countrycode'].isupper()
    recognised= record['recognized'] == True or record['recognized'] == False
    key_id = len(record['key_id']) == 16 and record['key_id'].isnumeric()

    drawing=record['drawing']
    var=len(drawing)>0
    for i in drawing :
        if var == True:
            if len(i) != 2:
               var=0
    return word and country and recognised and key_id and var

given_word = sys.argv[1]
for input1 in sys.stdin:
    try:
        dict_rec=json.loads(input1)
        is_clean = clean_record(dict_rec)
        if(is_clean):
          if(dict_rec['word'] == given_word):
            if(dict_rec['recognized']):
                print(dict_rec["word"], "\t", 1)
            else:

                day = datetime.datetime.strptime(dict_rec['timestamp'][:-4], '%Y-%m-%d %H:%M:%S.%f').weekday()
                day_correct = (day == 5 or day == 6)
                if(day_correct):
                    print(dict_rec["word"], "\t", 2)
    except:
        continue
