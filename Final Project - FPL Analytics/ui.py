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
import json
sc = SparkContext("local[2]", "FPL-notfromninad")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 1)
path=sys.argv[1]
###################################################################
def input_person(pid,predictions,chemistry_coeff,centers)

        df_filtered=predictions.filter(predictions.playerId==pid)

        if df_filtered.rdd.collect()[0].__getitem__("matchId") < 5 :
            cluster=df_filtered.rdd.collect()[0].__getitem__("prediction")
            values=centers[cluster]
            same_cluster_peeps=predictions.filter(predictions.prediction==cluster)
            average_coeff=0
            same_cluster_peeps=same_cluster_peeps.rdd.collect()
            for row in same_cluster_peeps:
                pid2=row.__getitem__("playerId")
                if pid > pid2:
                    temp=pid2
                    pid2=pid
                    pid=temp
                df_filtered=chemistry_coeff.filter((chemistry_coeff.player1 == pid ) & (chemistry_coeff.player2 == pid2 ))
                average_coeff+=df_filtered.rdd.collect()[0].__getitem__("chemistry")
            average_coeff/=len(same_cluster_peeps)
            predictions=predictions.withColumn("CHEMISTRY",F.when(F.col("playerId") == pid,F.lit(average_coeff)).otherwise(F.col("CHEMISTRY")))
            predictions=predictions.withColumn("RATING",F.when(F.col("playerId") == pid,F.lit(values[7])).otherwise(F.col("RATING")))
#####################################################################################
#path=input("Enter the complete file path: ")
spark = SparkSession.builder.appName('abc').getOrCreate()
player_csv= spark.read.csv('hdfs://localhost:9000/DATA/play.csv',inferSchema =True,header=True)
predictions = spark.read.csv('hdfs://localhost:9000/SPARK/predictions.csv',inferSchema=True,header=True)
new_player=predictions.join(player_csv, on = ["playerId"], how = "inner")
new_player.show()
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
        r1=[]
        r2=[]
        l1=[]
        l2=[]
        p=new_player.rdd.collect()
        team1name=team1["name"]
        team2name=team2["name"]
        del team1["name"]
        del team2["name"]
        for i in team1.values():
                for row in p:
                        #print(row.__getitem__("name"),i)
                        if(row.__getitem__("name")==i):
                                r1.append(row.__getitem__("role"))
                                l1.append(row.__getitem__("playerId"))

        for i in team2.values():
                for row in p:
                        #print(row.__getitem__("name"),i)
                        if(row.__getitem__("name")==i):
                                r2.append(row.__getitem__("role"))
                                l2.append(row.__getitem__("playerId"))
        print(l1,l2)
        if(check_team(r1) and check_team(r2)):
                print("TEAMS ARE ACCEPTED,perform regression now")
                #player_csv = spark.read.csv('hdfs://localhost:9000/SPARK/players.csv',inferSchema=True,header=True)
                #team_csv = spark.read.csv('hdfs://localhost:9000/SPARK/team.csv',inferSchema=True,header=True)

                new_player=new_player.filter((predictions.playerId in l1) | (predictions.playerId in l2))
                #new_player=predictions.join(player_csv, on = ["playerId"], how = "inner")

                if len(predictions.filter(new_player.matchId == 0).rdd.collect()) == 0:
                    print("“error“:	 “Invalid	 Player”")
                else:

                    new_player=new_player.withColumn("AGE",(F.datediff(F.lit(match_date),F.col("birthDate"))/365)**2)
                    lr_model = LinearRegression.load(sc,'hdfs://localhost:9000/SPARK/')
                    lr_predictions = lr_model.transform(new_player)

                    if len(lr_predictions.filter(lr_predictions.RATING < 0.2).rdd.collect()) != 0:
                        print("Has retired players")
                    else:
                        strength1=0
                        strength2=0
                        team1=lr_predictions.filter(lr_predictions.playerId in l1)
                        for row in team1.rdd.collect():
                            strength1+=row.__getitem__("CHEMISTRY")*row.__getitem__("RATING")
                        team2=lr_predictions.filter(lr_predictions.playerId in l2)
                        for row in team2.rdd.collect():
                            strength2+=row.__getitem__("CHEMISTRY")*row.__getitem__("RATING")
                        strength1/=11
                        strength2/=11
                        chance_of_winningA=(0.5+strength1-((strength1+strength2)/2))*100
                        chance_of_winningB=100-chance_of_winningA
                        print("WINNING CHANCES of A and B",chance_of_winningA,chance_of_winningB)
                        game=()
                        game["team1"]={"name":team1name,"winning chance":int(chance_of_winningA)}
                        game["team2"]={"name":team2name,"winning chance":int(chance_of_winningB)}

                        with open('result'+str(strength1)+'.json', 'w') as fp:
                            json.dump(game, fp)



        else:
                print("Invalid team")




def player_profile(data):
        given_name=data['name']
        prof=dict()
        #IF ALL CALCULATED VALUES ADDED TO play.csv, read from that and display
        that_player=new_player.filter(new_player.name == given_name)
        rec=that_player.rdd.collect()[0]

        prof["name"]=rec.__getitem__("name")
        prof["birthArea"]=rec.__getitem__("birthArea")
        prof["birthDate"]=rec.__getitem__("birthDate")
        prof["foot"]=rec.__getitem__("foot")
        prof["role"]=rec.__getitem__("role")
        prof["height"]=rec.__getitem__("height")
        prof["passportArea"]=rec.__getitem__("passportArea")
        prof["weight"]=rec.__getitem__("weight")

        if rec.__getitem__("matchId") != 0:
            prof["fouls"]=rec.__getitem__("fouls")
            prof["goals"]=rec.__getitem__("goals")
            prof["own_goals"]=rec.__getitem__("own_goals")
            prof["percent_pass_accuracy"]=int(rec.__getitem__("pass_accuracy")*100)
            prof["“percent_shots_on_target"]=int(rec.__getitem__("shot_effectiveness")*100)

        with open('result'+str(given_name)+'.json', 'w') as fp:
            json.dump(prof, fp)




def match_details(data):
        match_date=data['date']
        label=data["label"]
        match_profile = spark.read.csv('hdfs://localhost:9000/SPARK/match_profile.csv',inferSchema=True,header=True)
        #match=dict()
        match_profile=match_profile.withColumn("time",to_timestamp(F.col("time")))
        deets=match_profile.filter(match_profile.date == match_date)
        match=dict()
        match["date"]=rec.__getitem__("date")
        match["duration"]=rec.__getitem__("duration")
        match["winner"]=rec.__getitem__("winner")
        match["venue"]=rec.__getitem__("venue")
        match["gameweek"]=rec.__getitem__("gameweek")
        match["goals"]=[]
        g=rec.__getitem__("goals")
        for i in g.split("*"):
            l=i.split("+")
            if l[2] != '0':
                match["goals"].append({"name":l[0],"team":l[1],"goals":l[2]})

        match["yellow_cards"]=rec.__getitem__("yellow_cards")
        match["red_cards"]=rec.__getitem__("red_cards")
        with open('result'+str(match_date)+'.json', 'w') as fp:
            json.dump(prof, fp)



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
"""
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
       """
