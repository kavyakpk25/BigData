from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import (StructField, StringType, StructType, IntegerType,FloatType)

from pyspark.ml.regression import LinearRegression
def rename_columns_after_any_aggregation(df):
    for name in df.schema.names:
        clear_name = ''
        m = re.search('\((.*?)\)', name)
        if m:
            clear_name = m.group(1)
            df = df.withColumnRenamed(name, clear_name)

    return df

def input_person(pid,predictions,chemistry_coeff,centers)

        df_filtered=predictions.filter(predictions.playerId==pid)
        average_coeff=0
        if df_filtered.rdd.collect()[0].__getitem__("matchId") < 5 :
            cluster=df_filtered.rdd.collect()[0].__getitem__("prediction")
            values=centers[cluster]
            same_cluster_peeps=predictions.filter(predictions.prediction==cluster)

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
        else:
            length+=1
            fill=chemistry_coeff.filter((chemistry_coeff.player1 == pid) |(chemistry_coeff.player2 == pid) )
            for row in fill.rdd.collect():
                average_coeff+=row.__getitem__("CHEMISTRY")
                length+=1
            average_coeff/=length
            predictions=predictions.withColumn("CHEMISTRY",F.when(F.col("playerId") == pid,F.lit(average_coeff)).otherwise(F.col("CHEMISTRY")))
        return predictions



if(__name__=="__main__"):
	player_profile = spark.read.csv('hdfs://localhost:9000/SPARK/events.csv',inferSchema=True,header=True)
    #matchid playerid team date goals
	match_profile = spark.read.csv('hdfs://localhost:9000/SPARK/match_profile.csv',inferSchema=True,header=True)
    ratings = spark.read.csv('hdfs://localhost:9000/SPARK/player_rating.csv',inferSchema=True,header=True)
    player_profile=player_profile.join(ratings, on = ["playerId"], how = "inner")

	#df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
	#match_profile.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
    match_profile=match_profile.withColumn("time",to_timestamp(F.col("time")))
    player_profile=player_profile.join(match_profile, on = ["matchId","team","playerId"], how = "inner")

	#player_profile.groupby("playerId").avg("foul", "own_goal", "pass_accuracy", "shot_effectiveness")
	#spark1 = SparkSession.builder.appName('abc').getOrCreate()

    data_schema = [StructField('playerId', IntegerType(), True), StructField('goals', FloatType(), True),StructField("foul",FloatType(), True),StructField("own_goal", FloatType(), True),StructField("pass_accuracy", FloatType(), True),StructField("shot_effectiveness", FloatType(), True),StructField("RATING", FloatType(), True)]
    final_struc = StructType(fields = data_schema)
    final_profile=spark.createDataFrame([], schema=final_struc)
	list_players =player_profile.select("playerId").distinct().rdd.flatMap(lambda x: x).collect()
	for pid in list_players:
		df_filtered=df.filter(df.playerId==pid)
        df_filtered=df_filtered.orderBy("date")
        collected_rdd=df_filtered.rdd.collect()
        no_of_matches=len(collected_rdd)
        latest_rating=collected_rdd[-1].__getitem__("RATING")
        df_filtered.withColumn("matchId",F.lit("matchId"))
        df_filtered=df_filtered.withColumn("matchId",F.lit(no_of_matches))
        df_filtered=df_filtered.avg("matchId","goals","foul", "own_goal", "pass_accuracy", "shot_effectiveness","RATING")
        df_filtered=df_filtered.withColumn("RATING",F.lit(latest_rating))
        df_filtered=rename_columns_after_any_aggregation(df_filtered)
        final_profile=final_profile.union(df_filtered)
        #playerid "goals","foul", "own_goal", "pass_accuracy", "shot_effectiveness","RATING"


    chemistry_coeff = spark.read.csv('hdfs://localhost:9000/SPARK/chemistry_coeff.csv',inferSchema=True,header=True)
    chemistry_coeff.groupby("player1","player2").avg("chemistry")
    chemistry_coeff=rename_columns_after_any_aggregation(chemistry_coeff)


	print("FINAL PROFILE: ")
	final_profile.show()
	print(final_profile.schema)
	#df_load_match = spark.read.csv('hdfs://localhost:9000/SPARK/match.csv',inferSchema=True,header=True)
	#print("Matches: ")
	#df_load_match.show()
	print(df_load_match.schema)
	print()


    vecAssembler = VectorAssembler(inputCols=["matchId","playerId","goals","foul", "own_goal", "pass_accuracy", "shot_effectiveness","RATING"], outputCol="features")
    processed_df = vecAssembler.transform(final_profile)
    kmeans = KMeans(k=5, seed=1)  # 5 clusters here
    model = kmeans.fit(processed_df.select('features'))
    predictions = model.transform(processed_df)#same df with one more col called prediction
    centers = model.clusterCenters()#0th index is the first cluster
    predictions=predictions.withColumn("CHEMISTRY", F.lit(0))
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    for row in predictions.rdd.collect():

        predictions=input_person(row.__getitem__("playerId"),predictions,chemistry_coeff,centers)

    predictions.repartition(1).write.csv(path='hdfs://localhost:9000/SPARK/predictions.csv',header=True)


    player_csv = spark.read.csv('hdfs://localhost:9000/SPARK/players.csv',inferSchema=True,header=True)
    new_player=predictions.join(player_csv, on = ["playerId"], how = "inner")


    new_player=new_player.withColumn("AGE",(F.datediff(F.lit(datetime.datetime.now()),F.col("birthDate"))/365)**2)


	vectorAssembler = VectorAssembler(inputCols = ['AGE'], outputCol = 'features')
	new_df = vectorAssembler.transform(new_player)
	new_df = new_df.select(['features', 'RATING'])
	lr = LinearRegression(featuresCol = 'features', labelCol='MV', maxIter=10, regParam=0.3, elasticNetParam=0.8)
	lr_model = lr.fit(new_df)
	trainingSummary = lr_model.summary
	print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
	print("r2: %f" % trainingSummary.r2)
	lr_predictions = lr_model.transform(new_df)
    #to save
    lr_model.save(sc, 'hdfs://localhost:9000/SPARK/)
    #to load
    lr_model = LinearRegression.load(sc, 'hdfs://localhost:9000/SPARK/')
