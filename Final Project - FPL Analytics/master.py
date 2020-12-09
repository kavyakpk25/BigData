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
from pyspark.sql.types import StructType, StructField, StringType,LongType,IntegerType
from pyspark.sql.functions import to_timestamp

################################################################################################################################################
#Create a new player
def new_player(team):
			player=dict()
			player["t_and_goal"]=0
			player["t_an_nogoal"]=0
			player["total_shots"]=0
			player["accurate_normal_pass"]=0
			player["accurate_key_pass"]=0
			player["normal_pass"]=0
			player["key_pass"]=0
			player["fouls"]=0
			player["own_goal"]=0
			player["team"]=team
			player["duels_won"]=0
			player["neutral_duels"]=0
			player["total_duels"]=0
			player["effective_free_kicks"]=0
			player["penalties_scored"]=0
			player["total_free_kicks"]=0
			player["goals"]=0
			return player
#################################################################################################################################################
#create a new match record
def new_match(events):
        spark = SparkSession.builder.appName("Football Premier League - Analysis").getOrCreate()
        match_Id=events['wyId']
        MATCH_PROFILE=dict()
        MATCH_PROFILE['matchId']=events['wyId']
        MATCH_PROFILE['date']=str(events['dateutc'])
        MATCH_PROFILE['duration']=events['duration']
        winner= events["winner"]
        if(winner==0):
                winner=None
        MATCH_PROFILE['winner']=winner
        MATCH_PROFILE['venue']=events["venue"]
        MATCH_PROFILE['gameweek']=events["gameweek"]
        players=[]
        yellow_card_players=[]
        red_card_players=[]
        own_goals_list=[]
        goals_list=[]
        match_details=dict()
        goalsforplayers_list=[]
        if(events['duration']=='Regular'):
                ft=90
        else:
                ft=120
        for team in events['teamsData']:
                benched_players=events['teamsData'][team]['formation']['bench']
                substitutions=events['teamsData'][team]['formation']['substitutions']
                players=[i for i in events['teamsData'][team]['formation']['bench']]
                
                players.extend(events['teamsData'][team]['formation']['lineup'])
                for sub in substitutions:
                        IN=sub['playerIn']
                        Out=sub['playerOut']
                        time=0
                        if IN not in match_details:
                                match_details[IN]=dict()
                                match_details[IN]['matchId']=match_Id
                                match_details[IN]['team']=team
                                match_details[IN]['playerId']=IN
                                match_details[IN]['Time']=0
                        if Out not in match_details:
                                match_details[Out]=dict()
                                match_details[Out]['matchId']=match_Id
                                match_details[Out]['team']=team
                                match_details[Out]['playerId']=Out
                                match_details[Out]['Time']=0
                subs=[]
                for i in substitutions:
                        subs.append(i)
                final_subs=[]
                for i in subs:
                        final_subs.append([i['playerIn'],i['minute'],"in"])
                        final_subs.append([i["playerOut"],i["minute"],"out"])
                final_subs= sorted(final_subs, key = lambda x: (x[0], x[1]))
				#CALCULTING PLAY TIME FOR SUBSTITUTED PLAYERS
                for i in final_subs:
                        plid=i[0]
                        if match_details[plid]["Time"]==0:
                                if(i[2]=="out"):
                                        match_details[plid]["Time"]+=i[1]
                                else:
                                        match_details[plid]["Time"]+=(ft-i[1])
                        else:
                                if(i[2]=="in"):
                                        match_details[plid]["Time"]+=(ft-i[1])
                                if(i[2]=="out"):
                                        match_details[plid]["Time"]-=(ft-i[1])
				#ADDING BENCHED PLAYERS TO THE PLERS OF THAT MATCH
                for pl in benched_players:
                        
                        pl_Id=pl['playerId']
                        if pl_Id not in match_details:
                                match_details[pl_Id]=dict()
                                match_details[pl_Id]['matchId']=match_Id
                                match_details[pl_Id]['playerId']=pl_Id
                                match_details[pl_Id]['Time']=0
                                match_details[pl_Id]['team']=team
				#UPDATING MATCH_PROFILE
                for i in players:
                        if(i['redCards']!='0'):
                                red_card_players.append(str(i['playerId']))
                        if(i['yellowCards']!='0'):
                                yellow_card_players.append(str(i['playerId']))
                        if(i['goals']!='0'):
                                goals_list.append(str(i['playerId'])+"+"+team+"+"+i['goals'])                           
                        if(i['ownGoals']!='0'):
                                own_goals_list.append(str(i['playerId'])+"+"+team+"+"+i['ownGoals'])
                        goalforplayers_dict=dict()
                        goalforplayers_dict["playerId"]=i['playerId']
                        goalforplayers_dict["team"]=team
                        goalforplayers_dict["goals"]=i['goals']
                        goalforplayers_dict["matchId"]=events['wyId']
                        goalforplayers_dict["date"]=str(events['dateutc'])
                        goalsforplayers_list.append(goalforplayers_dict)     
        if(len(yellow_card_players)>0):
                MATCH_PROFILE['yellowcards']="+".join(yellow_card_players)
        else:
                MATCH_PROFILE['yellowcards']="No players"
        if(len(red_card_players)>0):
                MATCH_PROFILE['redcards']="+".join(red_card_players)
        else:
                MATCH_PROFILE['redcards']="No players"
        if(len(goals_list)>0):
                MATCH_PROFILE['goals']="*".join(goals_list)
        else:
                MATCH_PROFILE['goals']="No players"
        if(len(own_goals_list)>0):
                MATCH_PROFILE['owngoals']="*".join(own_goals_list)
        else:
                MATCH_PROFILE['owngoals']="No players"
        print("MATCH PROFILE COMPUTED")
        print("LENGTH OF MP: ", len(MATCH_PROFILE))
        print("KEYS: ", MATCH_PROFILE.keys())        
        df_goals=spark.createDataFrame(goalsforplayers_list)
        print("MATCH PROFILE",MATCH_PROFILE)        
        schema = StructType([StructField("date", StringType(), True),StructField("duration", StringType(), True),StructField("gameweek", LongType(), True),StructField("goals", StringType(), True),StructField("matchId", LongType(), True),StructField("owngoals", StringType(), True),StructField("redcards", StringType(), True),StructField("venue", StringType(), True),StructField("winner", LongType(), True),StructField("yellowcards", StringType(), True)])
        df2=spark.createDataFrame([MATCH_PROFILE],schema=schema)
        df2=df2.dropDuplicates()
        df2.repartition(1).write.csv('hdfs://localhost:9000/SPARK/match_profiles.csv',mode="append",header=True)
        df_goals=df_goals.dropDuplicates()
        df_goals.repartition(1).write.csv('hdfs://localhost:9000/SPARK/goalsforplayers.csv',mode="append",header=True)
        l=[]
        for i in match_details:
                l.append(match_details[i])
        return l
        
###############################################################################################################################################
#Helper Function
def sub_process_event(a,player):
	events = json.loads(a)
	if 'eventId' in events.keys():
		match_Id=events['matchId']
		player_Id=events['playerId']
		tags = [x['id'] for x in events['tags']]
		if(events['eventId'] == 8):

			if(len(events['tags']) > 0):
				player["accurate_normal_pass"] = 0

				player["normal_pass"] = 0
				key_passes = 0

				if 1801 in tags:
					player["accurate_normal_pass"] += 1
					player["normal_pass"] += 1
				if 1802 in tags:
					player["normal_pass"] += 1
				if 302 in tags and 1801 in tags:
					player["accurate_key_pass"] += 1
				if 302 in tags:
					player["key_pass"] += 1
		if(events['eventId'] == 1):
			if(len(events['tags']) > 0):
				player["duels_won"] = 0
				player["neutral_duels"]= 0
				total_duels = 0
				if 702 in tags:
					player["neutral_duels"] += 1
					player['total_duels'] += 1
				if 703 in tags:
					player["duels_won"] += 1
					player["total_duels"] += 1
				if 701 in tags:
					player["total_duels"] += 1
		if(events['eventId'] == 3):
			if(len(events['tags']) > 0):
				player["effective_free_kicks"]  = 0
				total_freekicks = 0
				if 1801 in tags:
					 player["effective_free_kicks"]+=1
					 player["total_free_kicks"] += 1
				if 1802 in tags:
					player["total_free_kicks"] += 1
				if(events['subEventId'] == 35):
					if 101 in tags:
						player["penalties_scored"] += 1
						
						
		if(events['eventId'] == 10):
			shot_effectiveness = 0
			if(len(events['tags']) > 0):
				if 1801 in tags and 101 in tags:
					player["t_and_goal"] += 1
					player["total_shots"] += 1
				elif 1801 in tags :
					player["t_an_nogoal"] += 1
					player["total_shots"] += 1
				elif 1802 in tags:
					player["total_shots"] += 1               
		if(events['eventId'] == 2):
			player["fouls"]+=1
		if(102 in tags):
			player["own_goal"]+=1
		if(101 in tags):
		        player["goal"]+=1
		        


#########################################################################################################################################################
#function passed for each record in the rdd - checks if event/match, redirects to correct functions
def create_data(a):
	events = json.loads(a)
	#IF EVENT RECORD
	if 'eventId' in events.keys():
		match_Id=events['matchId']
		player_Id=events['playerId']
		row=new_player(events['teamId'])
		row['playerId']=player_Id
		row["matchId"]=events['matchId']
		sub_process_event(a,row)
	#IF MATCH RECORD
	if 'wyId' in events.keys():
		row=new_match(events)
	return row

###########################################################################################################################################################
def convert_to_dictionary(a,match_details):
        print(type(a))
        a = a.replace("\'", "\"")
        match_details.update(simplejson.loads(a))
        return match_details


###########################################################################################################################################################
def rename_columns_after_any_aggregation(df):
    for name in df.schema.names:
        clear_name = ''
        m = re.search('\((.*?)\)', name)
        if m:
            clear_name = m.group(1)
            df = df.withColumnRenamed(name, clear_name)

    return df
############################################################################################################################################################
count=0
#Function passed for each rdd
def process(rdd):
        if(not rdd.isEmpty()):
                global count
                spark = SparkSession.builder.appName("Football Premier League - Analysis").getOrCreate()
                global count
                count+=1
                match_details=dict()
                match_rdd=rdd.map(lambda j: create_data(j))
                last_rdd=(match_rdd.take(match_rdd.count()))

                event_list=[]
                match_list=[]
                for i in last_rdd:
                        if not isinstance(i, dict):
                                match_list.extend(i)
                        else:
                                event_list.append(i)
                if(len(event_list)>0):
                        df = spark.createDataFrame(event_list)
                        df_events=df.groupby("matchId","playerId","team").sum("t_and_goal","t_an_nogoal","total_shots","accurate_normal_pass","normal_pass","key_pass","fouls","own_goal","duels_won","neutral_duels","total_duels","effective_free_kicks","penalties_scored","total_free_kicks")
                        df_events=rename_columns_after_any_aggregation(df_events)
                        df_events.show()
                        if(count==1):
                                df_events.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/events.csv',header=True)
                        else:
                                df_events.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/events.csv', mode="append")
                if(len(match_list)>0):
                        df_MATCH = spark.createDataFrame(match_list)
                        if(count==1):
                                df_MATCH.repartition(1).write.csv("hdfs://localhost:9000/SPARK/match.csv",header = True)
                        else:
                                df_MATCH.repartition(1).write.csv("hdfs://localhost:9000/SPARK/match.csv", mode="append")

#############################################################################################################################################################
if(__name__=="__main__"):
	#setMaster("local")
	sc = SparkContext("local[2]", "FPL")
	sqlContext = SQLContext(sc) 
	ssc = StreamingContext(sc, 1)
	spark = SparkSession.builder.appName("FPL").getOrCreate()
	#df = ssc.read.csv('hdfs://localhost:9000/Input/players.csv', header = True)
	#df2 = ssc.read.csv('hdfs://localhost:9000/Input/teams.csv', header = True)
	lines = ssc.socketTextStream("localhost", 6100) 
	lines.foreachRDD(lambda c: process(c))
	ssc.start()
	ssc.awaitTermination()

