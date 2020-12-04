'''User Interface tasks
The system must be able to undertake 3 tasks from the user. These tasks will in the form of
reading a JSON file with a request and writing a JSON file with the response:
1. Predicting match winning chances between 2 teams:
a. Users can provide a date of a match and 2 teams of 11 players each
b. System must ensure that the list of players have, as mentioned in the role field
of the player’s data:
1. 1 Goalkeeper (GK)
2. At least 3 defenders (DF)
3. At least 2 Mid fielders (MD)
4. And at least 1 Forward (FW)
c. Return Invalid team in case the above conditions are not met
d. After regression make sure that the rating of any player is not below 0.2, if any
player has a rating of less than 0.2, return that the player has retired
e. If all conditions are met, return chance of either team winning the match
'''
#!usr/local/python3
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext 
from pyspark.sql import Row, SQLContext
import sys
import json
from pyspark.sql import SparkSession
from functools import partial
import subprocess
import simplejson
import re
import pyspark.sql.functions as F
from pyspark.sql.functions import col
sc = SparkContext("local[2]", "FPL-notfromninad")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 1)
path=sys.argv[1]
#path=input("Enter the complete file path: ")
spark = SparkSession.builder.appName('abc').getOrCreate()
df_load = spark.read.csv('hdfs://localhost:9000/DATA/play.csv',inferSchema =True,header=True)
df_load.show() 
with open('teams.csv', newline='') as f:
    reader = csv.reader(f)
    data = [tuple(row) for row in reader]
print(data)
def check_team(l):
        gk=l.count('GK')
        df=l.count('DF')
        md=l.count('MD')
        fw=l.count('FW')
        if(gk==1 and df>=3 and md>=2 and fw>=1):
                return(1)
        else:
                return(0)
             
def predict_winning(data):
        match_date=data['date']
        team1=data["team1"]
        team2=data["team2"]
        l1=[]
        l2=[]
        p=df_load.rdd.collect()
        for i in team1.values():
                for row in p:
                        #print(row.__getitem__("name"),i)
                        if(row.__getitem__("name")==i):
                                l1.append(row.__getitem__("role"))
        for i in team2.values():
                for row in p:
                        #print(row.__getitem__("name"),i)
                        if(row.__getitem__("name")==i):
                                l2.append(row.__getitem__("role"))
        print(l1,l2)
        if(check_team(l1) and check_team(l2)):
                print("TEAMS ARE ACCEPTED,perform regression now")
        else:
                print("Invalid team")
                
                
                
                
def player_profile(data):
        name=data['name']
        #IF ALL CALCULATED VALUES ADDED TO play.csv, read from that and display


def match_details(data):
        match_date=data['date']
        label=data["label"]
        #RETURN DETAILS STUFF FROM MATCH PROFILE FILE


with open(path) as json_file:
        s = json_file.read()
        s = s.replace('\t','')
        s = s.replace('\n','')
        s = s.replace(',}','}')
        s = s.replace(',]',']')
        data = json.loads(s)
        #print(data)
        #data = simplejson.load(json_file)
        print(data)
        request_type=data['req_type']
        if(request_type==1):
                predict_winning(data)
        elif(request_path==2):
                player_profile(data)
        elif(request_type==3):
                match_details(data)
ssc.start()
ssc.awaitTermination() 




####################### MATCH PROFILE############################################################################

# NEED TO GET ALL CORRESPONDING TEAM NAMES AND PLAYER NAMES FROM play.csv and teams.csv - add below code to create data, and store on hadoop, read later
def infer_match(events):
        details={} #maybe add below stuff to this before writing
        date=events['dateutc']
        duration=events['duration']
        winner= events["winner"]
        if(winner==0):
                winner=None
        venue=events["venue"]
        gameweek=events["gameweek"]
        players=[]
        yellow_card_players=[]
        red_card_players=[]
        own_goal_list=[]
        goals_list=[]
        for team in teamsData:
                players=events['formation']['bench']
                players.extend[events['formation']['bench']]
                for i in players:
                        if(i['redCards']!=0):
                                red_card_players.append(i['playerId'])
                        if(i['yellowCards']!=0):
                                yellow_card_players.append(['playerId'])
                        if(i['goals']!=0):
                                goals_list.append((i['playerId'],team,i['goals']))
                        if(i['ownGoals']!=0):
                                own_goals_list.append((i['playerId'],team,i['goals']))       
       
