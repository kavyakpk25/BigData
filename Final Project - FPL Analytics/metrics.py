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

sc = SparkContext("local[2]", "FPL")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('FPL').getOrCreate()

#################################################################################################################################################################
def rename_columns_after_any_aggregation(df):
    for name in df.schema.names:
        clear_name = ''
        m = re.search('\((.*?)\)', name)
        if m:
            clear_name = m.group(1)
            df = df.withColumnRenamed(name, clear_name)
    return df

#################################################################################################################################################################
#CALCULATIONS OF RATING
events = spark.read.csv('hdfs://localhost:9000/SPARK/events_wd2.csv',inferSchema=True,header=True)
print("COUNT EVENTS:",events.count())
events.show()
print("READ EVENTS DONE")

match = spark.read.csv('hdfs://localhost:9000/SPARK/match_wd2.csv',inferSchema=True,header=True)
match.show()
print("COUNT MATCH",match.count())
print("READ MATCHES DONE")
	
goalsforplayers = spark.read.csv('hdfs://localhost:9000/SPARK/goalsforplayers_wd2.csv',inferSchema=True,header=True)
goalsforplayers.show()
print("COUNT GOALS",goalsforplayers.count())
print("READ GOALS DONE")

exprs = {x: "sum" for x in ["t_and_goal","t_an_nogoal","total_shots","accurate_normal_pass","normal_pass","key_pass","fouls","own_goal","duels_won","neutral_duels","total_duels","effective_free_kicks","penalties_scored","total_free_kicks"]}
player=events.groupBy("matchId","playerId","team").agg(exprs)
player=rename_columns_after_any_aggregation(player)
player= player.withColumn("pass_accuracy", F.when((F.col("normal_pass") + F.col("key_pass")*2)!=0,((F.col("accurate_normal_pass")+(F.col("key_pass")*2))/(F.col("normal_pass") + F.col("key_pass")*2))).otherwise(F.lit(0)))
player= player.withColumn("duel_effectiveness", (F.col("duels_won")+F.col("neutral_duels")*0.5)/(F.col("total_duels")))
player= player.withColumn("free_kick_effectiveness", (F.col("effective_free_kicks")+F.col("penalties_scored"))/(F.col("total_free_kicks")))
player= player.withColumn("shot_effectiveness", (F.col("t_and_goal")+(F.col("t_an_nogoal")*0.5))/(F.col("total_free_kicks")))
player=player.join(match,on=["matchId","playerId","team"],how="left")
player=player.fillna(90, subset=['Time'])
player= player.withColumn("CONTRIBUTION", ((F.col("pass_accuracy")+F.col("duel_effectiveness")+F.col("free_kick_effectiveness")+F.col("shot_effectiveness"))/4))
player=player.withColumn("CONTRIBUTION", F.when(F.col("Time") == 90,F.col("CONTRIBUTION")*1.05).otherwise(F.col("CONTRIBUTION")*F.col("Time")/90))
player=player.withColumn("PERFORMANCE", F.col("CONTRIBUTION")-0.05*F.col("fouls")-0.005*F.col("own_goal"))
player=player.fillna(0)

cols = ["playerId", "matchId","team"]
player=player.orderBy(cols, ascending=True)
l1=[]
l2=[]

count=0
rating=0.5
player_list=player.rdd.collect()
pid1=player_list[0].__getattr__('playerId')
for row in player_list[1:]:
        pid2=row.__getattr__('playerId')
        #count+=1
        if(pid2!=pid1):
                l2.append(d1)
                pid1=pid2
                rating=0.5                                         
        performance=row.__getattr__('PERFORMANCE')
        new_rating=round(((rating+performance)/2),5)
        change=round(new_rating-rating,5)
        d=dict()
        d["matchId"]=row.__getattr__('matchId')
        d["team"]=row.__getattr__('team')
        d["playerId"]=pid2
        d["change"]=change
        d["rating"]=new_rating
        d1=dict()
        d1["playerId"]=pid2
        d1["rating"]=new_rating
        l1.append(d)

print("LENGTH",len(l1),len(l2))
print(l1)
print(l2)
change_df=spark.createDataFrame(l1)
rating_df=spark.createDataFrame(l2)

change_df.repartition(1).write.csv("hdfs://localhost:9000/testing/final_player_change.csv",header = True)
rating_df.repartition(1).write.csv("hdfs://localhost:9000/testing/final_player_rating.csv",header = True)
player.repartition(1).write.csv("hdfs://localhost:9000/testing/final_player_metrics.csv",header = True)

print("Count",rating_df.count())
change_df.show()
print("Count",change_df.count())


################################################################################################################################################################
#CALCULATING CHEMISTRY COEFICIENT
def find_chem(match_details):
        visited=[]
        final=[]
        for i in match_details:
                for j in match_details:
                        if(str(i[0])+str(j[0]) not in visited and str(j[0])+str(i[0]) not in visited and i[0]!=j[0] and str(i[0])!="0" and str(j[0])!="0"):
                                visited.append(str(i[0])+str(j[0]))
                                avg1=round((abs(i[2])+abs(j[2]))/2,5)
                                d=dict()
                                d["matchId"]=i[1]
                                d["player1"]=i[0]
                                d["player2"]=j[0]
                                if(i[3]==j[3]):
                                        if((i[2]>0 and j[2]>0) or (i[2]<0 and j[2]<0)):
                                                d["coef"]=round(0.5+avg1,5)
                                        else:
                                                d["coef"]=round(0.5-avg1,5)
                                else:
                                        if((i[2]>0 and j[2]>0) or (i[2]<0 and j[2]<0)):
                                                d["coef"]=round(0.5-avg1,5)
                                        else:
                                                d["coef"]=round(0.5+avg1,5)
                                final.append(d)
        return(final)
        
        
##################################################################################################################################################################

change_df = spark.read.csv('hdfs://localhost:9000/testing/final_player_change.csv',inferSchema=True,header=True)
change_df.show()
print("COUNT",change_df.count())
cols = ["matchId","team"]
player=change_df.orderBy(cols, ascending=True)
player.show()
change_list=player.rdd.collect()
match_details=[]
final_list=[]
mid1=change_list[0].__getattr__('matchId')
match_details.append((change_list[0].__getattr__('playerId'),change_list[0].__getattr__('matchId'),change_list[0].__getattr__('change'),change_list[0].__getattr__('team')))
for row in change_list[1:]:
        mid2=row.__getattr__('matchId')
        if(mid2!=mid1):
                mid1=mid2
                print("MATCH",match_details)
                final_list.extend(find_chem(match_details))
                match_details=[]
        match_details.append((row.__getattr__('playerId'),row.__getattr__('matchId'),row.__getattr__('change'),row.__getattr__('team')))
        
print(final_list)
print("LENGTH",len(final_list))
coef_df=spark.createDataFrame(final_list)
coef_df.repartition(1).write.csv("hdfs://localhost:9000/testing/final_chem_coef.csv",header = True)

