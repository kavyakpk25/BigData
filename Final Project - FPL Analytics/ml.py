from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import (StructField, StringType, StructType, IntegerType,FloatType)
from pyspark import SparkConf, SparkContext
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import datetime

sc = SparkContext("local[2]", "FPL")
spark = SparkSession.builder.appName('FPL').getOrCreate()

#############################################################################################################################################################
def rename_columns_after_any_aggregation(df):
    for name in df.schema.names:
        clear_name = ''
        m = re.search('\((.*?)\)', name)
        if m:
            clear_name = m.group(1)
            df = df.withColumnRenamed(name, clear_name)

    return df

##############################################################################################################################################################
#Assigning chemistry coefficients to respective players that have less than five matches by averaging all values in the cluster

def clustering(predictions,chemistry_coeff,centers):
        cols = ["prediction", "matchId"]
        predictions=predictions.orderBy(cols, ascending=True)  
        pred_list=predictions.rdd.collect()     
        chem_list= chemistry_coeff.rdd.collect()
        #in ascending order of clusterno and no of matches played
        for row in pred_list:
            pid=row.__getitem__("playerId")
            average_coeff=0
            print(row)
            if row.__getitem__("matchId") < 5 :
                cluster=row.__getitem__("prediction")
                values=centers[cluster]
                #going throughthe same file to find all rows of same cluster
                for same_cluster in pred_list:
                    if same_cluster.__getitem__("prediction") == cluster:
                        pid2=row.__getitem__("playerId")
                        count=0
                        for i in chem_list:
                            if (((i.__getitem__("player1") == pid ) & (i.__getitem__("player2") == pid2 ))|((i.__getitem__("player1") == pid2 ) & (i.__getitem__("player2") == pid ))):
                                average_coeff+=i.__getitem__("coef")
                                count+=1
                    if count >0:
                        average_coeff/=count
                    predictions=predictions.withColumn("chemistry",F.when(F.col("playerId") == pid,F.lit(average_coeff)).otherwise(F.col("chemistry")))
                    predictions=predictions.withColumn("rating",F.when(F.col("playerId") == pid,F.lit(values[6])).otherwise(F.col("rating")))
            else:
                #length+=1
                count=0
                for i in chem_list:
                    if ((i.__getitem__("player1") == pid ) | (i.__getitem__("player2") == pid )):
                        average_coeff+=i.__getitem__("coef")
                        count+=1
                if count >0:
                    average_coeff/=count
                predictions=predictions.withColumn("chemistry",F.when(F.col("playerId") == pid,F.lit(average_coeff)).otherwise(F.col("chemistry")))
        return predictions
                       
################################################################################################################################################################ 
#Alternative function - same functionality as above                      
def assign_cluster_avg(matches_5,cluster_list,chem_list,centers,clust_num):
        l=[]
        i=0
        while(i<len(matches_5)):
                print(clust_num)
                row = matches_5[i]
                pid1=row.__getitem__("playerId")
                #cluster=row.__getitem__("prediction")
                total=0
                count=0
                matches_5.remove(row)
                for cluster in cluster_list:
                        print("in clust_list")
                        pid2=cluster.__getitem__("playerId")
                        for chem in chem_list:
                                if(((chem.__getitem__("player1") == pid1 ) & (chem.__getitem__("player2") == pid2 )) | ((chem.__getitem__("player1") == pid1 ) & (chem.__getitem__("player2") == pid2 ))):
                                        total+=chem.__getitem__("coef")
                                        count+=1
                for match in matches_5:
                        print("in match list")
                        pid2=match.__getitem__("playerId")
                        for chem in chem_list:
                                if(((chem.__getitem__("player1") == pid1 ) & (chem.__getitem__("player2") == pid2 )) | ((chem.__getitem__("player1") == pid1 ) & (chem.__getitem__("player2") == pid2 ))):
                                        total+=chem.__getitem__("coef")
                                        count+=1
                matches_5.insert(i,row) 
                i+=1
                d=dict()
                d["playerId"]=pid1
                d["coef"]=total/count
                d["rating"]=centers[clust_num][6]
                l.append(d)
        for cluster in cluster_list:
                pid=cluster.__getitem__("playerId")
                total=0
                count=0
                for chem in chem_list:
                                if((chem.__getitem__("player1") == pid ) | (chem.__getitem__("player2") == pid )):
                                        total+=chem.__getitem__("coef")
                                        count+=1
                d=dict()
                d["playerId"]=pid
                
                d["coef"]=total/count
                d["rating"]=cluster.__getitem__("rating")
                l.append(d)
        return(l)
              
def clustering1(predictions,chemistry_coeff,centers):
        cols = ["prediction", "matchId"]
        predictions=predictions.orderBy(cols, ascending=True)
        l=predictions.rdd.collect()
        chem_list=chemistry_coeff.rdd.collect()
        l1=[]
        c1=l[0].__getitem__("prediction")
        cluster_list=[]
        matches_5=[]
        for row in l[1:]:
                pid=row.__getitem__("playerId")
                c2=row.__getitem__("prediction")
                if c2!=c1:
                        l2=assign_cluster_avg(matches_5,cluster_list,chem_list,centers,c1)
                        l1.extend(l2)
                        c1=c2
                        del l2
                        del cluster_list
                        del matches_5
                        cluster_list=[]
                        matches_5=[]
                m=row.__getitem__("matchId")
                if(m<5):
                        matches_5.append(row)
                else:
                        cluster_list.append(row)
        return(l1)
        
########################################################################################################################################################
#MAIN Function     
if(__name__=="__main__"):
        player_profile = spark.read.csv('hdfs://localhost:9000/testing/final_player_metrics.csv',inferSchema=True,header=True)
        ratings = spark.read.csv('hdfs://localhost:9000/testing/final_player_rating.csv',inferSchema=True,header=True)
        player_profile=player_profile.drop("matchId")
        player_profile=player_profile.withColumn("matchId",F.lit(1))
        player_profile.show()
        #exprs = [F.mean(F.col(d)) for d in ["pass_accuracy","shot_effectiveness"]] + [F.sum(F.col(c)) for c in ["fouls","own_goal","matchId"]]
        exprs1 = {x: "sum" for x in ["fouls", "own_goal","matchId"]}
        player_profile_sum=player_profile.groupBy("playerId").agg(exprs1)
        player_profile_sum=rename_columns_after_any_aggregation(player_profile_sum)
        exprs2= {x: "avg" for x in ["pass_accuracy", "shot_effectiveness"]}
        player_profile_avg=player_profile.groupBy("playerId").agg(exprs2)
        player_profile_avg=rename_columns_after_any_aggregation(player_profile_avg)
        player_profile_sum.show()
        player_profile_avg.show()
        player_profile=player_profile_sum.join(player_profile_avg, on = ["playerId"], how = "inner")
        player_profile.show()
        print("COUNT",player_profile.count())
        player_profile=player_profile.join(ratings, on = ["playerId"], how = "inner")
        player_profile.show()
        print("COUNT",player_profile.count())
           
        chemistry_coeff = spark.read.csv('hdfs://localhost:9000/testing/final_chem_coef.csv',inferSchema=True,header=True)
        exprs = {x: "avg" for x in ["coef"]}
        chemistry_coeff=chemistry_coeff.groupBy("player1","player2").agg(exprs)
        chemistry_coeff=rename_columns_after_any_aggregation(chemistry_coeff)
        chemistry_coeff.show()
        
        vecAssembler = VectorAssembler(inputCols=["matchId","playerId","fouls", "own_goal", "pass_accuracy", "shot_effectiveness","rating"], outputCol="features")
        processed_df = vecAssembler.transform(player_profile)
        processed_df.show()
        
        # 5 clusters here
        kmeans = KMeans(k=5, seed=1)  
        model = kmeans.fit(processed_df.select('features'))
        predictions = model.transform(processed_df)#same df with one more col called prediction
        predictions.show()
        centers = model.clusterCenters()#0th index is the first cluster
        predictions=predictions.withColumn("chemistry", F.lit(0))
        predictions.show()
        print("Cluster Centers: ")
        for center in centers:
                        print(center)   
        predictions=clustering(predictions,chemistry_coeff,centers)
        predictions.show()
        predictions=predictions.drop("features")
        predictions.repartition(1).write.csv(path='hdfs://localhost:9000/testing/predictions.csv',header=True)        

        
