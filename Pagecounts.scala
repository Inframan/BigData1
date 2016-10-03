import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object Pagecounts 
{
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	

	def filterFunc(line: String) : Boolean = {
		var splitString: Array[String] = line.split(" ")
		val langCode = StringUtils.substringBefore(splitString(0), ".")

		return !langCode.equals(splitString(1))

	}

	//Converts original format to near output format (without language)
	//input format: <Language code> <Page title> <View count> <Page size> 
	//output format: <Languagecode>,<TotalViewsInThatLang>,<MostVisitedPageInThatLang>,<ViewsOfThatPage>
	def mapFunc(line: String) :  (String , String) = { 

		var splitString: Array[String] = line.split(" ")
		val langCode = StringUtils.substringBefore(splitString(0), ".")

	 	val ret = (langCode, langCode + " " + splitString(2) + " " + splitString(1) + " " + splitString(2)) 
	 	
		return ret
	}

	//Adds the language name for the output format
	def languAdd(line: String) : String = {
		val lang = getLangName(line.split(" ")(0))
		
		return lang + " " + line + "\n"
	}

	def reduceFunc( a: String, b: String) :  String = { 
	 	var asplit:Array[String] = a.split(" ")
	 	var bsplit:Array[String] = b.split(" ")

	 	var total:Int = asplit(1).toInt + bsplit(1).toInt	 	
	 	var ret:String = asplit(0) + " " + total + " " 

		if(asplit(3).toInt > bsplit(3).toInt)
		{
			ret += asplit(2) + " " + asplit(3)
		}
		else 
		{
			ret += bsplit(2) + " " + bsplit(3)
		}

		return ret
	 }


	def main(args: Array[String]) 
	{

		if(args.length != 1)
		{	
			args.foreach(println)
			System.err.println("Usage: run.sh <inputFile>")
			System.exit(1)
		}

		val inputFile = args(0) // Get input file's name from this command line argument
		val conf = new SparkConf().setAppName("Pagecounts")
		val sc = new SparkContext(conf)
		
		// Uncomment these two lines if you want to see a less verbose messages from Spark
		//Logger.getLogger("org").setLevel(Level.OFF);
		//Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here

		//open the file
        val pagecounts = sc.textFile(inputFile)

                                             
        val tuplesByLang = pagecounts.filter(filterFunc).map(line =>  mapFunc(line))


        val languagesViews = tuplesByLang.reduceByKey(reduceFunc)

       // languagesViews.foreach(println)


        val sorted = languagesViews.values.map(line => (line.split(" ")(1).toInt, line)).sortByKey(false)

        val outRDD = sorted.values.map(line => languAdd(line))
       
		// outRDD would have elements of type String.
		// val outRDD = ...
		// Write output
		val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
		bw.write("Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage\n")
		outRDD.collect.foreach(x => bw.write(x))
		bw.close
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

