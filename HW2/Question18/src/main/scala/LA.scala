import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType}


//sbt package
//spark-submit --class "LA" --driver-memory 6g --master local[*] target/scala-2.11/la-count_2.11-1.0.jar 
object schema{
	val struct = StructType (Array (StructField ("ID", StringType), StructField("City", StringType)))
}

object LA{
	def main (args: Array [String]){
		val sc = new SparkContext (new SparkConf().setAppName ("LA users"))
		val sqlContext = new org.apache.spark.sql.SQLContext (sc)
		import sqlContext.implicits._
		// Get All users from LA
		val users = sc.textFile ("users.txt")
		val LAusers = users.map (_.split ("\t")).map(attr => (attr(0), attr(1))).filter ((t) => t._2.startsWith("Los Angeles"))

		val tweets = sc.textFile ("tweets.txt")
		val tweetCount = tweets.filter ( line => line.contains ("2009-09-16") || 
							 line.contains ("2009-09-17") || 
							 line.contains ("2009-09-18") ||
							 line.contains ("2009-09-19") || 
							 line.contains ("2009-09-20") )
					.flatMap (line => line.split ("\t"))
					.flatMap (line => line.split (" "))
					.filter (word => word.matches ("""^\d{1,}\.*\d*$"""))
					.filter (word => word.length > 6)
					.map (word => (word,1))
					.reduceByKey(_+_)

		 val joined = LAusers.rightOuterJoin (tweetCount)
				     .map {case (k,(j,v)) => (k,v)}
				     .map (item =>item.swap)
				     .sortByKey (false,1)
				     .map (item =>item.swap)
				     .take (10)


		val results = sc.parallelize (joined)
		results.saveAsTextFile ("Question18")

	}
}
