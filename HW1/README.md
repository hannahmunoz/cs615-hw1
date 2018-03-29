# PreReqs
Hadoop 3.0.0
Java 1.8.4

# TO COMPILE
## cd into any Question * file and execute the following commands (WHERE HADOOP_HOME is the path to your Hadoop location)

javac -classpath `$HADOOP_HOME/bin/hadoop classpath` *.java
jar cvf [CLASS NAME].jar *.class

### default .jar include with each file is 
mr.jar

# QUESTIONS 5 AND 6
### creates intermeddiate file "outputfile" which will need to be deleted between runs
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar Driver tweets.txt [OUTPUT FILE 1]

# QUESTION 7
### creates intermeddiate files "tweetCountFile", "joinFile", and "countFile"  which will need to be deleted between runs
$HADOOP_HOME/bin/hadoop jar [CLASS NAME].jar Driver tweets.txt users.txt [OUTPUT FILE 1]

## retrieve results with 
hadoop fs -get [OUTPUT FILE 1]/part-r-00000





