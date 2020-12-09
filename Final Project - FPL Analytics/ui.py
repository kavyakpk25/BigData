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
from pyspark.sql import Row
from pyspark.ml.regression import LinearRegression
import datetime
import csv
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import to_timestamp
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import datetime


sc = SparkContext("local[2]", "FPL")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 1)
spark = SparkSession.builder.appName('FPL').getOrCreate()




############################################################################################################################################################
#Helper function to check if valid team
def check_team(l):
        gk=l.count('GK')
        df=l.count('DF')
        md=l.count('MD')
        fw=l.count('FW')
        if(gk==1 and df>=3 and md>=2 and fw>=1):
                return(1)
        else:
                return(0)
#########################################################################################################################################################
#Checks if given teams are valid, and if yes, goes further to retrive their winning chances
def predict_winning(data,new_player):
        print("IN PREDICT")
        match_date=data['date']
        team1=data["team1"]
        team2=data["team2"]
        r1=[]
        r2=[]
        l1=[]
        l2=[]
        p=player_csv.rdd.collect()
        team1name=team1["name"]
        team2name=team2["name"]
        del team1["name"]
        del team2["name"]
        for i in team1.values():
                for row in p:
                        #print(row.__getitem__("name"),i)
                        if(row.__getitem__("name")==i):
                                r1.append(row.__getitem__("role"))
                                l1.append(row.__getitem__("Id"))
        for i in team2.values():
                for row in p:
                        #print(row.__getitem__("name"),i)
                        if(row.__getitem__("name")==i):
                                r2.append(row.__getitem__("role"))
                                l2.append(row.__getitem__("Id"))
        print(l1,l2)
        if(check_team(r1) and check_team(r2)):
                print("TEAMS ARE ACCEPTED")
                p_collect=new_player.rdd.collect()
                teamlist=[]
                for row in p_collect:
                        if (row.__getitem__("playerId") in l1) or  (row.__getitem__("playerId") in l2):
                                teamlist.append(row)
                rdd1 = sc.parallelize(teamlist)
                test_df=rdd1.toDF()
                test_df=test_df.withColumn("AGE",(F.datediff(F.lit(datetime.datetime.now()),F.col("birthDate"))/365)**2)
                #new_player=new_player.filter((new_player.playerId in l1) | (new_player.playerId in l2))
                #new_player=predictions.join(player_csv, on = ["playerId"], how = "inner")
                df_1=new_player.filter(new_player.matchId == 0)
                df_l=df_1.rdd.collect()                
                if 0:
                    print("“error“:	 “Invalid	 Player”")
                else:
                        #new_player=new_player.withColumn("AGE",(F.datediff(F.lit(match_date),F.col("birthDate"))/365)**2)
                        #new_player=predictions.join(player_csv,(predictions.playerId == player_csv.Id),how = "inner")
                        new_player=new_player.withColumn("AGE",(F.datediff(F.lit(datetime.datetime.now()),F.col("birthDate"))/365)**2)
                        new_player=new_player.drop("prediction")
                        test_df=test_df.drop("prediction")
                        new_player.show()
                        vectorAssembler = VectorAssembler(inputCols = ['AGE'], outputCol = 'linear_features')
                        new_df = vectorAssembler.transform(new_player)
                        new_df.show()
                        new_df = new_df.select(['linear_features', 'rating'])
                        new_df.show()
                        lr = LinearRegression(featuresCol = 'linear_features', labelCol='rating', maxIter=10, regParam=0.3, elasticNetParam=0.8)
                        new_player.show()
                        lr_model1 = lr.fit(new_df)
                        trainingSummary = lr_model1.summary
                        print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
                        print("r2: %f" % trainingSummary.r2)
                        
                        vectorAssembler = VectorAssembler(inputCols = ['AGE'], outputCol = 'linear_features')
                        new_df = vectorAssembler.transform(test_df)
                                                                                         
                        lr_predictions = lr_model1.transform(new_df)
                        lr_predictions.show()
                        
                        if len(lr_predictions.filter(lr_predictions.prediction < 0.2).rdd.collect()) != 0:
                                print("Has retired players")
                        else:
                                strength1=0
                                strength2=0
                                p_collect=lr_predictions.rdd.collect()
                                team1=[]
                                team2=[]
                                for row in p_collect:
                                        if (row.__getitem__("playerId") in l1):
                                                team1.append(row)
                                        else:
                                                team2.append(row)
                    
                                for row in team1:
                                    strength1+=row.__getitem__("CHEMISTRY")*row.__getitem__("prediction")
                                for row in team2:
                                    strength2+=row.__getitem__("CHEMISTRY")*row.__getitem__("prediction")
                                strength1/=11
                                strength2/=11
                                chance_of_winningA=(0.5+strength1-((strength1+strength2)/2))*100
                                chance_of_winningB=100-chance_of_winningA
                                print("WINNING CHANCES of A and B",chance_of_winningA,chance_of_winningB)
                                game={}
                                game["team1"]={"name":team1name,"winning chance":int(chance_of_winningA)}
                                game["team2"]={"name":team2name,"winning chance":int(chance_of_winningB)}
                                print("\n\n\n\n\nFINAL JSON O/P:\n\n")
                                print(game)
                                with open('result'+str(strength1)+'.json', 'w') as fp:
                                        json.dump(game, fp)
                        
        else:
                print("Invalid team")

#############################################################################################################################################################
#Player profile - given player name, retrieves all relevant information about the player at the end of the tournament along with personal info ("IF the player has played")
def player_profile(data,new_player):
        given_name=data['name']
        prof=dict()
        #IF ALL CALCULATED VALUES ADDED TO play.csv, read from that and display
        that_player=new_player.filter(new_player.name == given_name)
        if(len(that_player.rdd.collect()!=0):
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
                    #prof["goals"]=rec.__getitem__("goals")
                    prof["own_goals"]=rec.__getitem__("own_goal")
                    prof["pass_accuracy"]=rec.__getitem__("pass_accuracy")
                    prof["shots_on_target"]=rec.__getitem__("shot_effectiveness")
                print("Final Profile: ")
                print(prof)
                with open('result'+str(given_name)+'.json', 'w') as fp:
                    json.dump(prof, fp)
        else:
                print("PLAYER NOT FOUND")

##############################################################################################################################################################
#Match profile - given a match date, retrieves its information from the stored database obtained during streaming ("If exists")
def match_details(data,teams_given):
        match_date=data['date']
        label=data["label"]
        match_profile = spark.read.csv('hdfs://localhost:9000/SPARK/match_profiles_wd2.csv',inferSchema=True,header=True)
        #match=dict()
        match_profile=match_profile.withColumn("date",(F.col("date")[0:10]))
        match_profile.show()
        match_profile=match_profile.fillna(0)               
        deets=match_profile.filter(match_profile.date == match_date)
        print(match_date)
        deets.show()
        if(len(deets.rdd.collect()!=0):    
                deets=deets.rdd.collect()[0]
                print(deets)
                match=dict()
                match["date"]=match_date
                match["duration"]=deets.__getitem__("duration")
                match["winner"]=deets.__getitem__("winner")
                match["venue"]=deets.__getitem__("venue")
                match["gameweek"]=deets.__getitem__("gameweek")
                match["goals"]=[]
                g=deets.__getitem__("goals")
                for i in g.split("*"):
                    l=i.split("+")
                    if l[2] != '0' and l[2]!='null':
                        for i in teams_given:
                                if(i[1]==l[1]):
                                        team=i[1]
                                else:
                                        team=l[1]
                        match["goals"].append({"name":l[0],"team":team,"goals":l[2]})

                match["yellow_cards"]=deets.__getitem__("yellowcards")
                match["red_cards"]=deets.__getitem__("redcards")
                print("THE MATCH DETAILS ARE")
                print(match)
                with open('result'+str(match_date)+'.json', 'w') as fp:
                        json.dump(match, fp)
        else:
                print("INVALID MATCH")
###########################################################################################################################################################
#MAIN Function
if(__name__=="__main__"):
        path=sys.argv[1]
        player_csv= spark.read.csv('hdfs://localhost:9000/testing/data/players.csv',inferSchema =True,header=True)
        predictions = spark.read.csv('hdfs://localhost:9000/testing/predictions.csv',inferSchema=True,header=True)
        team_csv=spark.read.csv('hdfs://localhost:9000/testing/data/teams.csv',inferSchema=True,header=True)
        new_player=predictions.join(player_csv,(predictions.playerId == player_csv.Id),how = "inner")
        new_player.show()
        with open(path) as json_file:
                s = json_file.read()
                s = s.replace('\t','')
                s = s.replace('\n','')
                s = s.replace(',}','}')
                s = s.replace(',]',']')
                data = json.loads(s)
                print("INPUT DATA: ")
                print(data)
                
                with open('/home/kavya/Documents/Final Project - FPL Analytics/teams.csv', newline='') as f:
                        reader = csv.reader(f)
                        teams_given = [tuple(row) for row in reader]
                        print(teams_given)
                try:
                        req=data['req_type']
                        if(req==1):
                                predict_winning(data,new_player)
                        elif(req==2):
                                player_profile(data,new_player)
                        elif(req==3):
                                match_details(data,teams_given)
                        else:
                                print("Invalid Input")
                except:
                        match_details(data,teams_given)
