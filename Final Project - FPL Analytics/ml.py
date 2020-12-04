from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler





vecAssembler = VectorAssembler(inputCols=["lat", "long"], outputCol="features")
new_df = vecAssembler.transform(df)
kmeans = KMeans(k=5, seed=1)  # 5 clusters here
model = kmeans.fit(new_df.select('features'))
predictions = model.transform(new_df)
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)   
    				
    		#EMPTY LIST WHILE WRITING 
    				
    			#goals, fouls , own goals , pass accuracy, shots on target	
    				
if(__name__=="__main__"):
	player_profile = spark.read.csv('hdfs://localhost:9000/SPARK/events.csv',inferSchema=True,header=True)
	
	match_profile = spark.read.csv('hdfs://localhost:9000/SPARK/match_profile.csv',inferSchema=True,header=True)
	player_profile=player_profile.join(match_profile, on = ["matchId"], how = "inner")
	from pyspark.sql.functions import to_timestamp
	df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
	df.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
	
	
	player_profile.groupby("playerId").avg("foul", "own_goal", "pass_accuracy", "shot_effectiveness")
	#spark1 = SparkSession.builder.appName('abc').getOrCreate()
	list_players = df.select("playerId").distinct().rdd.flatMap(lambda x: x).collect()
	for pid in list_records:
		df_filtered=df.filter(df.playerId==pid)
		
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
		                        
		                
	print("Events: ") 
	df_load_events.show()
	print(df_load_events.schema)
	#df_load_match = spark.read.csv('hdfs://localhost:9000/SPARK/match.csv',inferSchema=True,header=True)
	#print("Matches: ")
	#df_load_match.show()
	print(df_load_match.schema)
	print()

	from pyspark.ml.feature import VectorAssembler
	from pyspark.ml.regression import LinearRegression

	df = df.withColumn("AGE", F.col("AGE")**2)
	vectorAssembler = VectorAssembler(inputCols = ['AGE'], outputCol = 'features')
	new_df = vectorAssembler.transform(df)
	new_df = new_df.select(['features', 'Players Rating'])
	lr = LinearRegression(featuresCol = 'features', labelCol='MV', maxIter=10, regParam=0.3, elasticNetParam=0.8)
	lr_model = lr.fit(new_df)
	trainingSummary = lr_model.summary
	print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
	print("r2: %f" % trainingSummary.r2)
	lr_predictions = lr_model.transform(new_df) 																						
