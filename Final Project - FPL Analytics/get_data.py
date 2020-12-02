#!usr/local/python3
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext 
from pyspark.sql import Row, SQLContext
import sys
import json
from functools import partial
import subprocess

#setMaster("local")
sc = SparkContext("local[2]", "FPL-notfromninad")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 1)
#df = ssc.read.csv('hdfs://localhost:9000/Input/players.csv', header = True)
#df2 = ssc.read.csv('hdfs://localhost:9000/Input/teams.csv', header = True)
match_details=dict()
chemistry=dict()
lines = ssc.socketTextStream("localhost", 6100)

#words = lines.foreachRDD(lambda c: c["eventId"])

def new_player(team):
			player=dict()
			player["pass_accuracy"]=0
			player["duel_effectiveness"]=0
			player["free_kick_effectiveness"]=0
			player["shot_effectiveness"]=0
			player["fouls"]=0
			player["own_goal"]=0
			player["team"]=team
			player["PLAYER_CONTRIB"]=0
			player["PLAYER_PERF"]=0
			player["BENCHED"]=0
			player["PLAYER_RATING"]=0.5
			player["CHANGE"]=0.5
			player['Time']=90
			return player
def new_match(events):
			match_Id=events['wyId']
			for team in events['teamsData']:
				print()
				print("NEW MATCH CREATED !!!!!!!!!!!")
				print()
				benched_players=events['teamsData'][team]['formation']['bench']
				
				match_details[match_Id]['team'][team]=events['teamsData'][team]['formation']['substitutions']
				#I THINK THIS MIGHT BE WRONG
				for pl in benched_players:
					pl_Id=pl['playerId']
					match_details[match_Id][pl_Id]=new_player(team)
					match_details[match_Id][pl_Id]['BENCHED']=1
					match_details[match_Id][pl_Id]['Time']=0
			print("NEW MATCH_DETAILS IN SUBPROCESS !!!!!!!!!!!",match_details)

#{match ={player1:{},player2:{}}}######################################################
def create_data(a,match_details):
	print("IN CREATE DATA")
	events = json.loads(a)
	if 'eventId' in events.keys():
		match_Id=events['matchId']
		player_Id=events['playerId']
		
		if match_Id not in match_details:
			match_details[match_Id]=dict()
			match_details[match_Id]['team']=dict()
		if player_Id not in match_details[match_Id]:
			player=new_player(events['teamId'])
			match_details[match_Id][player_Id]=player
		
		sub_process(a,match_details[match_Id][player_Id])
	if 'wyId' in events.keys():
		print("NEW MATCH_DETAILS IN SUBPROCESS !!!!!!!!!!!",match_details)
		match_Id=events['wyId']
		
		if match_Id not in match_details:
			match_details[match_Id]=dict()
			match_details[match_Id]['team']=dict()
			
		new_match(events)
			
			
	player_statistics(match_details)
	return(match_details)		
	
					
				
			
					
				
		
		
		
########################################################################################################	

def sub_process(a,player):
	print("IN SUBPROCESS")
	events = json.loads(a)
	if 'eventId' in events.keys():
		pass_accuracy=0
		duel_effectiveness=0
		free_kick_effectiveness=0
		shot_effectiveness=0
		fouls=0
		own_goal=0
		match_Id=events['matchId']
		player_Id=events['playerId']
		j = [x['id'] for x in events['tags']]
		if(events['eventId'] == 8):
			pass_accuracy = 0  
			if(len(events['tags']) > 0):
				accurate_normal_passes = 0
				accurate_key_passes = 0
				normal_passes = 0
				key_passes = 0

				if 1801 in j:
					accurate_normal_passes += 1
					normal_passes += 1
				if 1802 in j:
					normal_passes += 1
				if 302 in j and 1801 in j:
					accurate_key_passes += 1
				if 302 in j:
					key_passes += 1
				#PASS ACCURACY
				pass_accuracy = (accurate_normal_passes+(accurate_key_passes*2))/(normal_passes + (key_passes*2))	
			if(events['eventId'] == 1):
				if(len(events['tags']) > 0):
					duel_won = 0
					neutral_duels = 0
					total_duels = 0
					if 702 in j:
						neutral_duels += 1
						total_duels += 1
					if 703 in j:
						duel_won += 1	
						total_duels += 1
					if 701 in j:
						total_duels += 1
					#duel_effectiveness
					duel_effectiveness = (duels_won + neutral_duels*0.5)/(total_duels)
			if(events['eventId'] == 3):
				if(len(events['tags']) > 0):
					effective_freekicks  = 0
					total_freekicks = 0
					penalty_scored 
					 
					if 1801 in j:
						 effective_freekicks+=1
						 total_freekicks += 1
					if 1802 in j:
						total_freekicks += 1	 
					if(events['subEventId'] == 35):
						if 101 in j:
							penalty_scored += 1
					#free_kick_effectiveness
					free_kick_effectiveness = (effective_freekicks + penalty_scored)/total_freekicks	
			if(events['eventId'] == 10):
				shot_effectiveness = 0
			if(len(events['tags']) > 0):
				t_and_goal = 0
				t_an_nogoal= 0
				total_shots= 0

				if 1801 in j and 101 in j:
					t_and_goal += 1
					total_shots += 1
				elif 1801 in j :
					t_an_nogoal += 1
					total_shots += 1
				elif 1802 in j:
					total_shots += 1
				#shot_effectiveness
				shot_effectiveness = (t_and_goal+t_an_nogoal*0.5 )/total_shots
			fouls = 0
			own_goal=0	
			if(events['eventId'] == 2):
				fouls+=1
			if(102 in j):
				own_goal+=1
			player["pass_accuracy"]=pass_accuracy
			player["duel_effectiveness"]=duel_effectiveness
			player["free_kick_effectiveness"]=free_kick_effectiveness
			player["shot_effectiveness"]=shot_effectiveness
			player["fouls"]=fouls
			player["own_goal"]=own_goal
			#print("MATCH_DETAILS IN SUBPROCESS !!!!!!!!!!!",match_details)
				
				
						
							
						 
				
						

#match_details{match1:{1:{},2:{},'team':{ist team:[substitions]}}}
#####PLAYER CONTRIBUTION##############
def player_statistics(match_details):
	for match in match_details:
		SUB_PLAYERS=list()
		if(len(match_details[match]['team']) > 0):
			for team in match_details[match]['team']:
				all_sub=match_details[match]['team'][team]
				for sub in all_sub:
					In=sub['playerIn']
					Out=sub['playerOut']
					
					#suppose adding them now
					if In not in match_details[match]:
						player=new_player(team)
						match_details[match][In]=player
						match_details[match][In]['Time']=0
					if Out not in match_details[match]:
						player=new_player(team)
						match_details[match][Out]=player
						match_details[match][Out]['Time']=0
					SUB_PLAYERS.append(In)
					SUB_PLAYERS.append(Out)
					match_Id=match	
					#in	
					if match_details[match_Id][In]["Time"]==0:
						match_details[match_Id][In]["BENCHED"]=1
						match_details[match_Id][In]["Time"]=90-sub['minute']
					else:
						match_details[match_Id][In]["BENCHED"]=1
						match_details[match_Id][In]["Time"]+=90-sub['minute']
					#out
					if match_details[match_Id][Out]["Time"]==90:
						match_details[match_Id][Out]["Time"]=sub['minute']
					else:
						match_details[match_Id][Out]["Time"]-=90-sub['minute']
			
			for player in match_details[match]:
				if player != 'team':	
					match_details[match][player]["PLAYER_CONTRIB"]=(match_details[match][player]["pass_accuracy"]+match_details[match][player]["duel_effectiveness"]+match_details[match][player]["free_kick_effectiveness"]+match_details[match][player]["shot_effectiveness"])/4
			
					if player not in SUB_PLAYERS:
						match_details[match][player]["PLAYER_CONTRIB"]*=1.05
					else:
						match_details[match][player]["PLAYER_CONTRIB"]*=match_details[match][player]["Time"]/90
				
					if match_details[match][player]['BENCHED']:
						match_details[match][player]["PLAYER_CONTRIB"]=0
			
				#Player	performance
					CONTRIBUTION=match_details[match][player]["PLAYER_CONTRIB"]
					match_details[match][player]["PLAYER_PERF"]=CONTRIBUTION-match_details[match][player]["fouls"]*0.005
					match_details[match][player]["PLAYER_PERF"]=CONTRIBUTION+match_details[match][player]["own_goal"]*0.05
					#Player	Rating
					z=match_details[match][player]["PLAYER_RATING"]
					
					match_details[match][player]["PLAYER_RATING"]=(match_details[match][player]["PLAYER_PERF"]+match_details[match][player]["PLAYER_RATING"])/2
					match_details[match][player]["CHANGE"]=match_details[match][player]["PLAYER_RATING"]-z
		#CHEMISTRY		
		for player in match_details[match] :	
			if player != 'team':
				player1_team=match_details[match][player]["team"]
				for player2 in match_details[match] :
					if player2 != 'team':
						if tuple(sorted((int(player),int(player2)))) not in chemistry:
							chemistry[tuple(sorted((int(player),int(player2))))]=0.5
						key=tuple(sorted((int(player),int(player2))))
						avg=abs(match_details[match][player]["CHANGE"]+match_details[match][player2]["CHANGE"])*0.5
						if match_details[match][player2]["team"] != player1_team:
							
							if ((match_details[match][player]["CHANGE"] > 0 and match_details[match][player2]["CHANGE"]> 0) or (match_details[match][player]["CHANGE"] > 0 and match_details[match][player2]["CHANGE"]< 0)):
								chemistry[key]-=avg
							else:
								chemistry[key]+=avg
						else:
							if ((match_details[match][player]["CHANGE"] > 0 and match_details[match][player2]["CHANGE"]> 0) or (match_details[match][player]["CHANGE"] > 0 and match_details[match][player2]["CHANGE"]< 0)):
								chemistry[key]+=avg
							else:
								chemistry[key]-=avg
				print()
				#print("CHEMISTRY   ",chemistry)
				print()
##########################################################################
def convert_to_dictionary(a,match_details):
        print(type(a))
        a = a.replace("\'", "\"")
        match_details.update(json.loads(a))
        print(json.loads(a))
count=0
##############################################################################################################		
def process(rdd):
        if(not rdd.isEmpty()):
                global count
                tf = sc.textFile('hdfs://localhost:9000/SPARK/rec'+str(count)+'.txt')
                
                count+=1
                #some_path = "/SPARK/rec.txt"
                #subprocess.call(["/home/kavya/hadoop", "fs", "-rm", "-f", some_path])
                #match_details=sqlContext.read.json('hdfs://localhost:9000/SPARK/rec.json')
                print("PROPERLY READ")
                global match_details
                match_details=dict()
                tf.foreach(partial(convert_to_dictionary, match_details=match_details))
                #data = tf.map(lambda x: json.loads(x))
                #print("\n\n\n\n\nDATA COLLECTED",data.collect())
                print("\n\n\n\n\n")
                #data=dict(data)
                #if(len(data)==0):
                #        match_details=dict()
                #else:
                #        match_details = dict(data)
                print("THIS IS WHAT I GOT FROM HADOOP",type(match_details))
                print(match_details)
                #match_details=match_details.rdd.map(lambda row: row.asDict(True))
                #print("THIS IS AFTER CONVERTING",type(match_details))
                #rdd.foreach(create_data)
                match_rdd=rdd.map(lambda j: create_data(j,match_details))
                #rdd.foreach(partial(create_data, arg1=match_details))
                print("TYPE of MATCH DETAILS AFTER CREATE DATA",type(match_details))
                print(match_details)
                #match_details.persist()
                #rdd.cache()
                print()
                print("CHEMISTRY AFTER A DSTREAM   ",chemistry)
                print()
                #match_details.write.json('hdfs://localhost:9000/SPARK/rec.json')
                match_rdd.coalesce(1).saveAsTextFile('hdfs://localhost:9000/SPARK/rec'+str(count)+'.txt')
                #sqlContext.read.json(sc.parallelize([match_details])).coalesce(1).write.json('hdfs://localhost:9000/SPARK/rec.json')
                print("PROPERLY WRITTEN")	
                print()
                #print("MATCH DETAILS LENGTH ",len(match_details))
                #print("MATCH DETAILS",match_details)		

	
lines.foreachRDD(lambda c: process(c))
#print(match_details)
#rdd1=sc.parallelize(match_details.values())
#df=sqlContext.createDataFrame(rdd1,list(match_details.keys()))
#df.write.json('hdfs://localhost:9000/SPARK/rec.json')
#df = sqlContext.read.json('rec.json')

#my_RDD_strings = sc.textFile('rec.json')
#my_RDD_dictionaries = my_RDD_strings.map(json.loads)
#print(my_RDD_dictionaries)

#df=json.loads(df)
def print_rdd(a):
	print("NEW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",i)

   	

print()
print()
#print(type(lines))
print()
print()
#data = json.loads(lines)

#rint(df)
#recong_event = 'eventId'
##if recog_event in data.keys():
#lines.pprint()
ssc.start()
ssc.awaitTermination() 


