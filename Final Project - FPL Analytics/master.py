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
#from pyspark.sql.functions import when
#from pyspark.sql.functions import lit
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
			return player

def new_match(events):
		#match_details={playerId,matchId,team,time},{}
		#MATCH_PROFILE={matchId:,date,duration,winner,venue,gameweek,red,yellow,goal,own_goal}
        spark2 = SparkSession.builder.appName('abc').getOrCreate()
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
                        if(i['redCards']!=0):
                                red_card_players.append(i['playerId'])
                        if(i['yellowCards']!=0):
                                yellow_card_players.append(i['playerId'])
                        if(i['goals']!=0):
                                goals_list.append((i['playerId'],team,i['goals']))
                        if(i['ownGoals']!=0):
                                own_goals_list.append((i['playerId'],team,i['goals']))
        MATCH_PROFILE['yellowcards']=yellow_card_players
        MATCH_PROFILE['redcards']=red_card_players
        MATCH_PROFILE['goals']=goals_list
        MATCH_PROFILE['owngoals']=own_goals_list
        
        
        
        json_match = json.dumps(MATCH_PROFILE)
        
        with open("sample.json", "w") as outfile:
        	outfile.write(json_match)
        #print()
        #print("*******************")
        #print("MATCH PROFILE COLUMN NAMES", MATCH_PROFILE.schema.names)
        #print("*********************")
        #df2=spark2.createDataFrame([MATCH_PROFILE])
        #names = df2.schema.names
        #for name in names:
        #	print(name , 'NULL COUNT: ' , df2.where(df2[name].isNull()).count())
        #	print(name , 'NOT NULL COUNT: ' , df2.where(df2[name].isNotNull()).count())
        
        #df2.toJSON().saveAsTextFile("hdfs://localhost:9000/matches/"+str(match_Id)+".json")
        #df2.repartition(1).write.json(path='hdfs://localhost:9000/SPARK/match_profiles.json', mode="append")
        
        #df2.repartition(1).write.mode('append').json('hdfs://localhost:9000/SPARK/match_profiles.json')
        #json_match.repartition(1).write.mode('append').json('hdfs://localhost:9000/SPARK/match_profiles.json')
	        #OBTAINING A LIST OF DICTIONARY OF SUB AND BENCHED PLAYER
        l=[]
        for i in match_details:
                l.append(match_details[i])
        return l
###################################################################################################

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


##########################################################################
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
		#print("I AM A ROWWW",row,type(row))

	return row

########################################################################################################
def chemistry_coeff(df):
        #spark1 = SparkSession.builder.appName('abc').getOrCreate()
        matches = df.select("matchId").distinct().rdd.flatMap(lambda x: x).collect()
        for matchId in matches:
                df_filtered=df.filter(df.matchId==matchId)
                d=dict()
                for player in df_filtered.rdd.collect():
                        for player2 in df_filtered.rdd.collect():
                                p1=player.__getitem__("playerId")
                                p2=player2.__getitem__("playerId")
                                t1=player.__getitem__("team")
                                t2=player2.__getitem__("team")
                                c1=player.__getitem__("CHANGE")
                                c2=player2.__getitem__("CHANGE")
                                if((p1,p2) not in d and (p2,p1) not in d and p1!=p2):
                                        d[(p1,p2)]=[0.5,t1,t2,c1,c2]
                for x in d:
                        t1=d[x][1]
                        t2=d[x][2]
                        c1=d[x][3]
                        c2=d[x][4]
                        avg1=(abs(c1)+abs(c2))/2
                        if(t1==t2):
                                if((c1>0 and c2>0) or (c1<0 and c2<0)):
                                        d[x][0]+=avg1
                                else:
                                        d[x][0]-=avg1
                        else:
                                if((c1>0 and c2>0) or (c1<0 and c2<0)):
                                        d[x][0]-=avg1
                                else:
                                        d[x][0]+=avg1

                l=[]
                for x in d:
                        final_dict=dict()
                        final_dict["player1"]=x[0]
                        final_dict["player2"]=x[1]
                        final_dict["chemistry"]=d[x][0]
                        final_dict["matchId"]=matchId
                        l.append(final_dict)
                #print(l)
                return(l)


##########################################################################################
def player_statistics(player,match):
        #spark1 = SparkSession.builder.appName('abc').getOrCreate()
        df2=player.groupby("matchId","playerId","team").sum("t_and_goal","t_an_nogoal","total_shots","accurate_normal_pass","normal_pass","key_pass","fouls","own_goal","duels_won","neutral_duels","total_duels","effective_free_kicks","penalties_scored","total_free_kicks")

        player=rename_columns_after_any_aggregation(df2)

        player= player.withColumn("pass_accuracy", F.when((F.col("normal_pass") + F.col("key_pass")*2)!=0,((F.col("accurate_normal_pass")+(F.col("key_pass")*2))/(F.col("normal_pass") + F.col("key_pass")*2))).otherwise(F.lit(0)))
        player= player.withColumn("duel_effectiveness", (F.col("duels_won")+F.col("neutral_duels")*0.5)/(F.col("total_duels")))
        player= player.withColumn("free_kick_effectiveness", (F.col("effective_free_kicks")+F.col("penalties_scored"))/(F.col("total_free_kicks")))
        player= player.withColumn("shot_effectiveness", (F.col("t_and_goal")+(F.col("t_an_nogoal")*0.5))/(F.col("total_free_kicks")))
        #player_contrib
        print("****************************1")
        print("CONTRIBUTION")
        print("*******************************")
        player=player.join(match,on=["matchId","playerId","team"],how="left")
        player=player.fillna(90, subset=['Time'])
        player= player.withColumn("CONTRIBUTION", ((F.col("pass_accuracy")+F.col("duel_effectiveness")+F.col("free_kick_effectiveness")+F.col("shot_effectiveness"))/4))
        player=player.withColumn("CONTRIBUTION", F.when(F.col("time") == 90,F.col("CONTRIBUTION")*1.05).otherwise(F.col("CONTRIBUTION")*F.col("time")/90))
        player=player.withColumn("PERFORMANCE", F.col("CONTRIBUTION")-0.05*F.col("fouls")-0.005*F.col("own_goal"))
        player=player.fillna(0)
        print("**************************2")
        print("PERFORMANCE DONE")
        print("*****************************")
        #on forums it says 10%
        #player rating
        #our assumtion id matchId is sequential=>check if it is sequential
         
        
        player=player.withColumn("CHANGE", F.lit(0))
        player = player.sort(F.col("matchId").asc())
        prev_mid = player.rdd.collect()[0].__getitem__("matchId")
        #player_data = player.rdd.collect()
        #player_rating = player.groupby("playerId").avg("RATING")
        p_r = dict()
        print("********************************3")
        print("BEFORE FOR LOOP")
        print("**********************")
        i = 0
        #list_players = player.select("playerId").distinct().rdd.flatMap(lambda x: x).collect()
	#for pid in list_records:
	#	df_filtered=df.filter(df.playerId==pid)
        #player.groupby("playerId").avg("foul", "own_goal", "pass_accuracy", "shot_effectiveness")
        for row in player.rdd.collect():
        	
                #__getitem__
                pid = row.__getitem__("playerId")
                print("PID", pid)
                print("I", i)
                mid = row.__getitem__("matchId")
                team = row.__getitem__("team")
                if pid not in p_r.keys():
                        p_r[pid]=dict()
                        #p_r[pid]["CHANGE1"] = 0
                        p_r[pid]["playerId"] = pid
                        p_r[pid]["RATING"] = 0.5

                old_rating = p_r[pid]["RATING"]
                p_r[pid]["RATING"] = (p_r[pid]["RATING"] + row.__getitem__("PERFORMANCE"))/2
                new_rating = p_r[pid]["RATING"]
                change = new_rating - old_rating
                print("CHANGE", change)
                #p_r[pid]["CHANGE1"]=change               
                player=player.withColumn("CHANGE", F.when((F.col("matchId") == mid) & (F.col("team") == team) & (F.col("playerId") == pid),change).otherwise(F.col("CHANGE")))
                i = i+1

        #WRITING THE FINAL COMPUTED METRICS
        list_df = [x for x in p_r.values()]
        
        return(player,list_df)
        
##########################################################################
def convert_to_dictionary(a,match_details):
        print(type(a))
        a = a.replace("\'", "\"")
        match_details.update(simplejson.loads(a))
        return match_details


############################################################################################################
def rename_columns_after_any_aggregation(df):
    for name in df.schema.names:
        clear_name = ''
        m = re.search('\((.*?)\)', name)
        if m:
            clear_name = m.group(1)
            df = df.withColumnRenamed(name, clear_name)

    return df
##########################################################################
count=0
def process(rdd):
        if(not rdd.isEmpty()):
                global count
                spark = SparkSession.builder.appName('abc').getOrCreate()
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

                #print("Count of fouls")
                #count_df=df_load.groupby("matchId").sum("fouls","own_goal")
                #df_load = spark.read.csv('hdfs://localhost:9000/SPARK/events.csv')
                #print("SHAPE 2", df_load.count())
                #df_load = spark.read.csv('hdfs://localhost:9000/SPARK/match.csv')
                #print("SHAPE 1", df_load.count())


if(__name__=="__main__"):
	#setMaster("local")
	sc = SparkContext("local[2]", "Football Premier League - Analysis")
	sqlContext = SQLContext(sc)
	ssc = StreamingContext(sc, 1)
	#df = ssc.read.csv('hdfs://localhost:9000/Input/players.csv', header = True)
	#df2 = ssc.read.csv('hdfs://localhost:9000/Input/teams.csv', header = True)
	#lines = ssc.socketTextStream("localhost", 6100)
	#lines.foreachRDD(lambda c: process(c))
	spark = SparkSession.builder.appName('abc').getOrCreate()
	df_load_events = spark.read.csv('hdfs://localhost:9000/SPARK/events.csv',inferSchema=True,header=True)
	print("Events: ")
	df_load_events.show()
	print(df_load_events.schema)
	df_load_match = spark.read.csv('hdfs://localhost:9000/SPARK/match.csv',inferSchema=True,header=True)
	print("Matches: ")
	df_load_match.show()
	print(df_load_match.schema)
	
	player,ratings_list=player_statistics(df_load_events,df_load_match)
	player_rating_Df = spark.createDataFrame(ratings_list)
	player=player.join(player_rating_Df,on=["playerId"],how="inner")
	player.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/player_metrics.csv', mode="append")
	#player.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/player_metrics.csv', mode="append")
	#COMPUTING THE CHEMISTRY FOR ALL MATCHES
	'''
	chem=chemistry_coeff(player)
	chem_df=spark.createDataFrame(chem)
	chem_df.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/chemistry.csv', mode="append")
	'''
	
	#ssc.start()
	#ssc.awaitTermination()

