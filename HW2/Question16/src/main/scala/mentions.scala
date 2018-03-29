import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//sbt package
//spark-submit --class "mentions" --master yarn target/scala-2.11/mention-counts_2.11-1.0.jar
//spark-submit --class "mentions" --driver-memory 6g --master local[*] target/scala-2.11/mention-counts_2.11-1.0.jar 

object mentions{
	def main (args: Array [String]){
		val sc = new SparkContext (new SparkConf().setAppName ("Mention Count"))
		val tweets = sc.textFile ("tweets.txt")

		val results = tweets.map (_.replaceAll ("""[,./?!'-):]"""," "))
				    .flatMap (line =>line.split ("""[\p{Space}]"""))
                                    .filter (word => word.startsWith ("@"))
				    .flatMap (line =>line.split ("@"))
				    .distinct
				    .filter (_.nonEmpty)
				    .map (word => (word, 1))
				    .reduceByKey (_+_)
				    .map (item => item.swap)
				    .sortByKey (false,1).map (item =>item.swap)

		results.saveAsTextFile ("Question16")

	}
}
