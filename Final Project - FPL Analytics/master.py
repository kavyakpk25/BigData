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
#from pyspark.sql.functions import when
#from pyspark.sql.functions import lit
from pyspark.sql.functions import to_timestamp
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
        #spark2 = SparkSession.builder.appName('abc').getOrCreate()
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
                
                
     
      
        
        '''
        json_match = json.dumps(MATCH_PROFILE)
        
        with open("sample.json", "w") as outfile:
        	outfile.write(json_match)
        	GOALS ADD MATCH ID, DATE
        	
        '''
        #print()
        #print("*******************")
        #print("MATCH PROFILE COLUMN NAMES", MATCH_PROFILE.schema.names)
        #print("*********************")
        print("MATCH PROFILE COMPUTED")
        print("LENGTH OF MP: ", len(MATCH_PROFILE))
        print("KEYS: ", MATCH_PROFILE.keys())
        
        
        df_goals=spark.createDataFrame(goalsforplayers_list)
        #names = df2.schema.names
        #for name in names:
        #	print(name , 'NULL COUNT: ' , df2.where(df2[name].isNull()).count())
        #	print(name , 'NOT NULL COUNT: ' , df2.where(df2[name].isNotNull()).count())
        
        #df2.toJSON().saveAsTextFile("hdfs://localhost:9000/matches/"+str(match_Id)+".json")
        #df2.repartition(1).write.json(path='hdfs://localhost:9000/SPARK/match_profiles.json', mode="append")
        print("MATCH PROFILE",MATCH_PROFILE)
        
        schema = StructType([StructField("date", StringType(), True),StructField("duration", StringType(), True),StructField("gameweek", LongType(), True),StructField("goals", StringType(), True),StructField("matchId", LongType(), True),StructField("owngoals", StringType(), True),StructField("redcards", StringType(), True),StructField("venue", StringType(), True),StructField("winner", LongType(), True),StructField("yellowcards", StringType(), True)])
        df2=spark.createDataFrame([MATCH_PROFILE],schema=schema)
        df2=df2.dropDuplicates()
        df2.repartition(1).write.csv('hdfs://localhost:9000/SPARK/match_profiles.csv',mode="append",header=True)
        
        #print("WROTE TO MP CSV")
        #print("DF2 COUNT", df2.count())
        #print("WITH DUPLICATES")
        #df2.show()
        #print("WITHOUT DUPLICATES")
        #df2.dropDuplicates().show()
        #print("DF2 COUNT WITHOUT DUPLICATES", df2.dropDuplicates().count())
        #print("I WROTE HERE") 
        df_goals=df_goals.dropDuplicates()
        df_goals.repartition(1).write.csv('hdfs://localhost:9000/SPARK/goalsforplayers.csv',mode="append",header=True)
        #print("WROTE TO GOALS CSV")
        #print("DF_GOALS COUNT", df_goals.count())
        #print("WITH DUPLICATES")
        #df_goals.show()
        #print("WITHOUT DUPLICATES")
        #df_goals.dropDuplicates().show()
        #print("DF_GOALS COUNT WITHOUT DUPLICATES", df_goals.dropDuplicates().count())
        
        #print("I WROTE HERE") 
        #print("WROTE MATCH AND GOALS")
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
                        if(x[0]>x[1]):
                                temp=x[0]
                                x[0]=x[1]
                                x[1]=temp
                        final_dict=dict()
                        final_dict["player1"]=x[0]
                        final_dict["player2"]=x[1]
                        final_dict["chemistry"]=d[x][0]
                        final_dict["matchId"]=matchId
                        l.append(final_dict)
                #print(l)
                return(l)


##########################################################################################
def player_statistics(player,match,match_profile):
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
        player=player.withColumn("CONTRIBUTION", F.when(F.col("Time") == 90,F.col("CONTRIBUTION")*1.05).otherwise(F.col("CONTRIBUTION")*F.col("Time")/90))
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

        print("********************************3")
        print("BEFORE FOR LOOP")
        print("**********************")
        i = 0
        
        match_profile=match_profile.withColumn("",to_timestamp(F.col("date")))
        player=player.join(match_profile.select("matchId","date","playerId","team"), on = ["matchId","team","playerId"], how = "inner")
        list_players = player.select("playerId").distinct().rdd.flatMap(lambda x: x).collect()
        for p in list_players:
                df_filtered= player.filter( player.playerId==p)
                df_filtered=df_filtered.sort(F.col("date").asc())
                p_r = dict()
                print("ITERATION: ",i)
                print("PLAYER ID:", p)
                for row in df_filtered.rdd.collect():
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
                list_df = [x for x in p_r.values()]
                chem_df=spark.createDataFrame(list_df)
                chem_df.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/player_rating.csv', mode="append")
        #WRITING THE FINAL COMPUTED METRICS

        #for pid in list_records:
        #
        #player.groupby("playerId").avg("foul", "own_goal", "pass_accuracy", "shot_effectiveness")


        return player
        
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
                
                #print("Count of fouls")
                #count_df=df_load.groupby("matchId").sum("fouls","own_goal")
                #df_load = spark.read.csv('hdfs://localhost:9000/SPARK/events.csv')
                #print("SHAPE 2", df_load.count())
                #df_load = spark.read.csv('hdfs://localhost:9000/SPARK/match.csv')
                #print("SHAPE 1", df_load.count())
                '''
                if(count==2):
                        print("LOADING events")
                        df_load_matchprofiles=spark.read.csv('hdfs://localhost:9000/SPARK/events.csv',inferSchema=True,header=True)
                        df_load_matchprofiles.show()
                        print(df_load_matchprofiles.schema)
                        print(df_load_matchprofiles.count())
                        
                        print("LOADING matches")
                        df_load_goalsforplayers=spark.read.csv('hdfs://localhost:9000/SPARK/match.csv',inferSchema=True,header=True)
                        df_load_goalsforplayers.show()
                        print(df_load_goalsforplayers.schema)
                        print(df_load_goalsforplayers.count())                        
                        exit()
                '''
                       


if(__name__=="__main__"):
	#setMaster("local")
	sc = SparkContext("local[2]", "Football Premier League - Analysis")
	sqlContext = SQLContext(sc) 
	#ssc = StreamingContext(sc, 1)
	spark = SparkSession.builder.appName("Football Premier League - Analysis").getOrCreate()
	#df = ssc.read.csv('hdfs://localhost:9000/Input/players.csv', header = True)
	#df2 = ssc.read.csv('hdfs://localhost:9000/Input/teams.csv', header = True)
	#lines = ssc.socketTextStream("localhost", 6100) 
	#lines.foreachRDD(lambda c: process(c))
	
	df_load_events = spark.read.csv('hdfs://localhost:9000/SPARK/events_wd.csv',inferSchema=True,header=True)
	print("Events: ")
	df_load_events.show()
	print(df_load_events.count())
	df_load_match = spark.read.csv('hdfs://localhost:9000/SPARK/match_wd.csv',inferSchema=True,header=True)
	print("Events: ")
	df_load_match.show()
	print(df_load_match.count())
	
	goalsforplayers = spark.read.csv('hdfs://localhost:9000/SPARK/goalsforplayers_wd2.csv',inferSchema=True,header=True)
	goalsforplayers.show()
	print(goalsforplayers.count())
	player=player_statistics(df_load_events,df_load_match,goalsforplayers)
	player.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/player_metrics.csv',header=True)
	'''
	schema=StructType([StructField("matchId",IntegerType(),True),StructField("playerId",IntegerType(),True),StructField("team",IntegerType(),True),StructField("t_and_goal",IntegerType(),True),StructField("t_an_nogoal",IntegerType(),True),StructField("total_shots",IntegerType(),True),StructField("accurate_normal_pass",IntegerType(),True),StructField("accurate_key_pass",IntegerType(),True),StructField("normal_pass",IntegerType(),True),StructField("key_pass",IntegerType(),True),StructField("fouls",IntegerType(),True),StructField("own_goal",IntegerType(),True),StructField("duels_won",IntegerType(),True),StructField("neutral_duels",IntegerType(),True),StructField("total_duels",IntegerType(),True),StructField("effective_free_kicks",IntegerType(),True),StructField("penalties_scored",IntegerType(),True),StructField("total_free_kicks",IntegerType(),True)])

	df_load_events = spark.read.csv('hdfs://localhost:9000/SPARK/events.csv',header=False,schema=schema)
	print("Events: ")
	df_load_events.show()
	print(df_load_events.schema)
	'''
	#df_load_match = spark.read.csv('hdfs://localhost:9000/SPARK/match.csv',inferSchema=True,header=True)
	#print("Matches: ")
	#df_load_match.show()
	#print(df_load_match.schema)
	#player=player_statistics(df_load_events,df_load_match)
	#player=player.join(player_rating_Df,on=["playerId"],how="inner")
	#player.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/player_metrics.csv', mode="append")
	
	#COMPUTING THE CHEMISTRY FOR ALL MATCHES
	'''
	chem=chemistry_coeff(player)
	chem_df=spark.createDataFrame(chem)
	chem_df.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/chemistry.csv', mode="append")
	'''
	
	#ssc.start()
	#ssc.awaitTermination()

