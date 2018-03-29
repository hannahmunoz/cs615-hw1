import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//sbt package
//spark-submit --class "RT" --driver-memory 6g --master local[*] target/scala-2.11/rt-counts_2.11-1.0.jar 

object RT{
	def main (args: Array [String]){
		val sc = new SparkContext (new SparkConf().setAppName ("Popular RT Count"))
		val tweets = sc.textFile ("tweets.txt")

		val results = tweets.map (_.replaceAll ("""[,./?!'-):]"""," "))
				    .flatMap (line =>line.split ("RT "))
                                    .filter (word => word.startsWith ("@"))
				    //.filter (_.nonEmpty)
				    .flatMap (line =>line.split (" "))
                                    .filter (word => word.startsWith ("@"))
				    .filter (word => word.length > 1)
				    .map (word => (word, 1))
				    .reduceByKey (_+_)
				    .map (item => item.swap)
				    .sortByKey (false,1).map (item =>item.swap)
				    .take (10) 
		val results2 = sc.parallelize (results)
		results2.saveAsTextFile ("Question17")

	}
}
