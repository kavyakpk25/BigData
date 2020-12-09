# BigData
Course related assignments and project

Assignment 1 - Count using Map Reduce

Assignment 2 - Page rank using map reduce - tested on part of Google data set

Assignment 3 - Spark applications


------------------------------------------------------------------------------------------------------------------------------------------------------------------

<b> Final Project - FPL Analytics - Spark Streaming Project </b>


*Project Details:*

This project is based on SPARK streaming & Spark Processing.

<b> Aim </b>: 

To derive required information from streaming data obtained from the Football Premier League, with respect to player performances, ratings, and match profiles, and retrieve this information /predict chances of winning based on input data.

To run this project, the following steps are followed:

1) From hadoop directory:

     $  sbin/start-dfs.sh
     
     $  sbin/start-yarn.sh
     

2) Retrieval of info - run the following files simultaneously

     $  python3 stream.py (from the folder which contains)
    
     $  spark-submit master.py
   
   
3) Calculating primary metrics, performance, contribution, along with change in ratings of players between matches, and their final rating. Also calculating chemistry between all players who have played with/against each other using the changes in their ratings.

     $  spark-submit metrics.py
     

4) Clustering - Performing k means clustering to group the players into 5 groups. Further, checking players who have played less than 5 matches, and updating their ratings using the average of chemistry coefficients of all players in their respective clusters.

     $  spark-submit ml.py
    
    
5) UI - This file is to be run after the running of all above steps, along with input json file as a command line parameter. Final output is written into locl system.

     $  spark-submit ui.py "your-input-file.json"
     
     
    
