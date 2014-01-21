import com.twitter.scalding._
import java.io.PrintWriter

/* Goals: Find finance related tweets, uid & date from firehodse data
*/

class FinanceTweets(args:Args) extends Job(args) {
  val junk = (0,"","","", "")

  val regex = """^\$[A-Z]{1,5}""" // cashtag regex

  // tweets from 9am = 09 , to 4pm = 16, on Jan 13, 2014, available as flat Tsvs
  val tweets = List("09.tsv","10.tsv", "11.tsv", "12.tsv", "13.tsv", "14.tsv", "15.tsv", "16.tsv")

  def isFinanceTweet(tweet:String):Option[String] = {
    val arr = tweet.toUpperCase.split("\\s")
      .filter( i => i.matches(regex))
    if (arr.size > 0) Some(arr(0)) else None
  }

  val pipes = for(filename <- tweets) yield {
    TextLine(filename)
    .map('line -> ('id, 'user_id, 'tweet, 'created_at, 'ticker)){
      x:String =>
      val arr = x.split("\t")
      if (arr.size < 4 ) junk
      else {
        val (id, user_id, tweet, created_at) = (arr(0), arr(1), arr(2), arr(3))
        val ticker = isFinanceTweet(tweet)
        if (ticker.isDefined) (id, user_id, tweet, created_at, ticker.get)
        else junk
      }
    }
    .discard('offset, 'line)
    .filter('id) { x:Long => x != 0 }
  }

  pipes.reduce(_ ++ _)
    .groupBy('ticker){
      group => group
        .toList[String]('id -> 'id)
        .toList[String]('user_id -> 'user_id)
        .toList[String]('tweet -> 'tweet)
        .toList[String]('created_at -> 'created_at)
    }.flatMap(('id, 'user_id, 'tweet, 'created_at, 'ticker) -> ('id, 'user_id, 'tweet, 'created_at, 'ticker)) {
      x:(List[String], List[String], List[String], List[String], String) =>
      val (id, user_id, tweet, created_at, ticker) = x
      val pw = new PrintWriter(ticker.substring(1) + ".txt")
      for( i <- 0 until id.size) {
        pw.printf("%20s\t%20s\t%140s\t%s\n", id(i), user_id(i), tweet(i), created_at(i))
      }
      pw.flush
      pw.close
      for( i <- 0 until id.size) yield {
        (id(i), user_id(i), tweet(i), created_at(i), ticker)
      }
    }.write(Tsv("tweets"))
}
