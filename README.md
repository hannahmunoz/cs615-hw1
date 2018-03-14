# PreReqs
Hadoop 3.0.0
Java 1.8.4

# TO COMPILE
## cd into any Question * file and execute the following commands (Assuming the HADOOP .bashrc variables are correct)

javac -classpath `$HADOOP_HOME/bin/hadoop classpath` *.java
jar cvf [CLASS NAME].jar *.class

### default .jar include with each file is 
mr.jar

# QUESTIONS 5 AND 6
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar Driver tweets.txt [OUTPUT FILE 1]
### count the top N results 
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar topDriver [OUTPUT FILE 1]/part-r-00000 [OUTPUT FILE 2]

## retrieve results with 

hadoop fs -get [OUTPUT FILE 2]/part-r-00000

# QUESTION 7
### get number of tweets per user
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar userTweetDriver tweets.txt [OUTPUT FILE 1]
### join with cities
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar joinDriver users.txt [OUTPUT FILE 1]/part-r-00000 [OUTPUT FILE 2]
### keep total tweets per city
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar cityDriver [OUTPUT FILE 2]/part-r-00000 [OUTPUT FILE 3]
### count the top N results 
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar topDriver [OUTPUT FILE 3]/part-r-00000 [OUTPUT FILE 4]




