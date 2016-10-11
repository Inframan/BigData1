import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{ 
	var firstTime = true
	var t0: Long = 0
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))

	case class SimplifiedStatus(var originalId:Long, var content:String, var lang:String, var id:Long, var lowerBound:Long, var upperBound:Long, var retweets:Long ) {
		
	}


	//stores the necessary attributes of twitter4j.Status in SimplifiedStatus
	//return a tuple of originalId, simple
	def mapRetweetsById(status : twitter4j.Status) : (Long, SimplifiedStatus) =
	{
		
		val originalId = status.getRetweetedStatus.getId
		val retweetsCount = status.getRetweetedStatus().getRetweetCount()
		val text = status.getText()
		val lang = getLang(text) 
		val id = status.getId()

		val simple = new SimplifiedStatus(originalId, text,lang,id, retweetsCount, retweetsCount, 0)
		
		return (originalId, simple)

	}

	//Updates the SimplifiedStatus retweet count
	//Returns a tuple of Language-code and SimplifiedStatus
	def mapLangs(status: SimplifiedStatus) : (String, SimplifiedStatus) = {
		
		status.retweets = status.upperBound - status.lowerBound + 1

		return (status.lang, status)
	}


	//Input is the key (which is the language)
	//The left element contains the language's total retweets
	//The righ element contains the tweet itself
	//Output has the language's total retweers as key, and the write2Log input format:
	// (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount)
	def mapOuterJoin(line: (String, (Option[SimplifiedStatus], Option[SimplifiedStatus]))) : (String, Long, Long, String, Long, Long)  = {
		

		val left = line._2._1.get
		val right =  line._2._2.get

		
		var  tup:(String, Long, Long, String, Long, Long) = (left.lang,left.retweets, right.originalId,	 right.content, right.upperBound, right.lowerBound)
		

		return tup

	}



	def reduceLangs(a: SimplifiedStatus, b: SimplifiedStatus) : SimplifiedStatus = {
		if(a.retweets < b.retweets)
		{
			a.retweets += b.retweets				
			return a
		}
		else
		{
			b.retweets += a.retweets
			return b
		}
	}

	

	def reduceRetweets(a: SimplifiedStatus, b: SimplifiedStatus) : SimplifiedStatus =
	{
		if(a.upperBound > b.upperBound)
		{
			a.lowerBound = b.lowerBound			
			return a
		}
		else{
			b.lowerBound = a.lowerBound
			
			return b
		}

	}
	
	// This function will be called periodically after each 5 seconds to log the output. 
	// Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount)
	def write2Log(a: Array[(String, Long, Long, String, Long, Long)])
	{
		if (firstTime)
		{
			bw.write("Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)	
			{
				val langCode = a(i)._1
				val lang = getLangName(langCode)
				val totalRetweetsInThatLang = a(i)._2
				val id = a(i)._3
				val textStr = a(i)._4.replaceAll("\\r|\\n", " ")
				val maxRetweetCount = a(i)._5
				val minRetweetCount = a(i)._6
				val retweetCount = maxRetweetCount - minRetweetCount + 1
				
				bw.write("(" + seconds + ")," + lang + "," + langCode + "," + totalRetweetsInThatLang + "," + id + "," + 
					maxRetweetCount + "," + minRetweetCount + "," + retweetCount + "," + textStr + "\n")
			}
		}
	}
  
	// Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
  
	def main(args: Array[String]) 
	{
		// Configure Twitter credentials
		val apiKey = "ylMD9p01ej99BOHBYyZGfwZnk"
		val apiSecret = "nPH5uJOhEPLqZFaJvQSYA3pOSeaQkngPJrfz41n4dgdYrUhrud"
		val accessToken = "1569267878-zBhZFGF7ThUNuFZQfabwTdQeaAN2AE54rgwaEJ2"
		val accessTokenSecret = "hfboUA6gF1FGglycp6xFhvqmh1vGInvpWG1m3Azag6dhm"
		
		Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
		
		val ssc = new StreamingContext(new SparkConf(), Seconds(5))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		// Add your code here
		
		//tweets.foreachRDD(_.take(100).println)
		//Filters the tweets int the stream, leaving only retweets
 		val retweets = tweets.filter(status => status.isRetweet())

 		//maps every retweet its original tweet id
 		val mapedRetweetsByOriginalId = retweets.map(status => mapRetweetsById(status)) 

 		//reduces every retweet with the same key to have one with the lowestBound highestBound
        val counts = mapedRetweetsByOriginalId.reduceByKeyAndWindow(reduceRetweets(_,_), Seconds(60), Seconds(5))

        //maps the remainning retweets to its language code
        val groupByLang = counts.map(status => mapLangs(status._2) )

        //reduces every language to the single most retweeted tweet
        val maxLangCount = groupByLang.reduceByKeyAndWindow(reduceLangs(_,_), Seconds(60), Seconds(5))

        //Joins the most retweeted tweet in a language with every other tweet in the same language
        val everything = maxLangCount.fullOuterJoin(groupByLang).filter(line => !(line._2._1.isEmpty || line._2._2.isEmpty))
        	.map(line => mapOuterJoin(line))
		
		
        everything.transform(rdd=>rdd.sortBy( r => (r._2, r._5 - r._6 +1), false))

        everything.foreachRDD(x =>  write2Log(x.collect ))
		//everything.foreachRDD(_.sortByKey(false).foreach(x => println(x._1+ " => "+x._2.deep.mkString(" "))))

         
 //       val sortedCounts = counts.map {case(tag, count) => (count, tag)}.transform(rdd => rdd.sortByKey(false))
   //     sortedCounts.foreachRDD(rdd => println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))

		// If elements of RDDs of outDStream aren't of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount),
		//	then you must change the write2Log function accordingly.
		//outDStream.foreachRDD(rdd => write2Log(rdd.collect))
	
		new java.io.File("cpdir").mkdirs
		ssc.checkpoint("cpdir")
		ssc.start()
		ssc.awaitTermination()
	}
}

