/**
  * Created by chanti on 08-Feb-16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object WordCount5540 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\chanti\\Downloads\\Programs\\hadoop-winutils-2.6.0")
    // initialise spark context
    val conf = new SparkConf().setAppName("WordCountSpark").setMaster("local[2]").set("spark.executor.memory","2g")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val tweets = sqlContext.jsonFile("E:/study/MS/Sem 1/BigData/increment2/tweetsfile.json")
    tweets.registerTempTable("tweetsTable")
   // tweets.printSchema();
    tweets.collect()

    //Query 1
    //val q1 = sqlContext.sql("select place.country, count(*) as countrycount from tweetsTable GROUP by place.country order by countrycount desc limit 10")
    //q1.show()
    //q1.save("output1","json")

    //Query 2
    //val q2 = sqlContext.sql("select user.name, user.followers_count from tweetsTable where (user.name != NULL or user.name NOT LIKE '%.%' or user.name NOT LIKE '%,%'  or user.name != ',') order by followers_count desc limit 20")
    //q2.show()
    //q2.save("output2","json")

    //Query 3
    //val q3 = sqlContext.sql("SELECT user.location, COUNT(*) AS android_count FROM tweetsTable WHERE source LIKE '%android%' GROUP BY user.location ORDER BY android_count DESC LIMIT 10")
    //q3.show()
    //q3.save("output3","json")

    //Query 4
    //val q4 = sqlContext.sql("SELECT user.location, COUNT(*) AS ios_count FROM tweetsTable WHERE source LIKE '%iphone%' GROUP BY user.location ORDER BY ios_count DESC LIMIT 10")
    //q4.save("output4","json")

    //Query 5
    //val q5 = sqlContext.sql("SELECT retweeted_status.user.name, max(retweeted_status.retweet_count) AS retweet_count FROM tweetsTable GROUP BY retweeted_status.user.name ORDER BY retweet_count DESC LIMIT 10")
    //q5.show()
    //q5.save("C:\\Users\\chanti\\Downloads\\Programs\\output.json")
    //q5.save("output5","json")

    //Query 6
    //val q6 = sqlContext.sql("SELECT place.country, count(*) as CountryCount from tweetsTable Group by place.country order by CountryCount desc limit 50")
    //q6.save("output6", "json")

    //Query 7
    //val q7 = sqlContext.sql("SELECT user.profile_text_color, count(*) as textColorCount from tweetsTable Group by user.profile_text_color order by textColorCount desc limit 20")
    //q7.show()
    //q7.save("output7", "json")

    //Query 8
   // val q8 = sqlContext.sql("SELECT SUBSTRING(user.created_at, 27,4), count(*) as userCount from tweetsTable group by SUBSTRING(user.created_at, 27,4) order by userCount desc limit 15")
    //q8.show()

    //Query 9
    val q9 = sqlContext.sql("SELECT user.id, user.friends_count as friendsCount from tweetsTable order by friendsCount desc limit 15")
    q9.show()
    q9.save("output9", "json")
  }
}
