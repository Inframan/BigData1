import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object AnalyzeTwitters
{

	//Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text
	case class Tweet(var langCode:String, var id:Long, var upperBound:Long, var lowerBound:Long, var text:String, var totalRetweetsInThatLang:Long, var retweets:Long)
	{

	}

	def mapTweet(line: String) : (Long, Tweet) = {
		 

		val l_split = line.split(",")
		var tweet:Tweet = Tweet(l_split(2), l_split(4).toLong, l_split(5).toLong, l_split(6).toLong, l_split(8), 0, 0)


		return (tweet.id , tweet)
	}

	def mapLang(tweet: Tweet) : (String, Tweet) = {
		

		tweet.retweets = tweet.upperBound - tweet.lowerBound + 1
		tweet.totalRetweetsInThatLang = tweet.retweets 

		return (tweet.langCode,tweet)
	}

	def mapOuterJoin(line: (String, (Option[Tweet], Option[Tweet]))) : (Long, Tweet) = {
		

		val left = line._2._1.get
		val right =  line._2._2.get

		
		var  tweet:Tweet = Tweet(left.langCode,right.id,right.upperBound, right.lowerBound, right.text, left.totalRetweetsInThatLang, right.retweets )
		var l:Long  = left.totalRetweetsInThatLang

		return (l, tweet)

}

	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}

	def reduceById(a: Tweet, b: Tweet ) : Tweet = {
		
		if(a.lowerBound > b.lowerBound)
			a.lowerBound = b.lowerBound

		if(a.upperBound < b.upperBound)
			a.upperBound = b.upperBound	

		return a
	}

	def reduceByLang(a: Tweet, b: Tweet) : Tweet = {
		

		a.totalRetweetsInThatLang += b.totalRetweetsInThatLang

		return a
	}



	
	def main(args: Array[String]) 
	{
		val inputFile = args(0)
		val conf = new SparkConf().setAppName("AnalyzeTwitters")
		val sc = new SparkContext(conf)
		
		// Comment these two lines if you want more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here
		
													//Filters the first line of the file (header)
		 val analyzeTweets = sc.textFile(inputFile).filter(!_.startsWith("Seconds") )

		 val mappedById = analyzeTweets.map(line => mapTweet(line))

		 val tweetCount = mappedById.reduceByKey(reduceById(_,_))

		 val mappedByLang = tweetCount.map(line => mapLang(line._2))

		 val reduceLang = mappedByLang.reduceByKey(reduceByLang(_,_))

		 val sorted = reduceLang.fullOuterJoin(mappedByLang).map(line => mapOuterJoin(line))

		 val outRDD = sorted.filter(line => line._2.retweets >2).sortBy(r => (r._2.totalRetweetsInThatLang, r._2.retweets), false)

		// outRDD would have elements of type String.
		// val outRDD = ...
		// Write output
		val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
		bw.write("Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")
		outRDD.collect.foreach(x => bw.write(getLangName(x._2.langCode) + "," + x._2.langCode + ","+ x._2.totalRetweetsInThatLang.toString  + ","+ x._2.id.toString + "," + x._2.retweets.toString + "," + x._2.text +"\n"))
		bw.close
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

