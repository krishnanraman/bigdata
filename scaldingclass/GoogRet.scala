import com.twitter.scalding._

/* Goals: Find the daily returns of goog in 2013
Keywords:
toList
flatMapTo
sum
*/

class GoogRet(args:Args) extends Job(args) {
  val src = Tsv(args("input"), ('Date,'Open, 'High, 'Low, 'Close, 'Volume))

  // number of days in 2013 goog gained, lost, remained unchanged
  src.read
  .filter('Date) {
    x:String => x.endsWith("-13")
  }
  .project('Date, 'Close)
  .groupAll{
    group =>
    group.toList[String]('Date -> 'Datelist).toList[Double]('Close -> 'CloseList)
  }.mapTo(('Datelist, 'CloseList) -> ('dateprices)){
    x:(List[String], List[Double]) =>
    val (dates, prices) = x

    def lt(a:(String,Double), b:(String, Double)) = {
      val arr = a._1.split("-")
      val brr = b._1.split("-")
      (arr(0).toInt < brr(0).toInt) && (arr(1) == brr(1)) && (arr(2) == brr(2))
    }
    val dp = dates.zip(prices).sortWith(lt)
    val (d,p) = dp.unzip
    d.zip(p.zip(p.tail))
  }
  //.write(Tsv("dateprices"))
  .flatMapTo('dateprices -> ('date, 'returns)){
    x:List[(String,(Double,Double))] =>
    x.map{ i =>
      val (date, prices) = i
      val returns = math.log(prices._2) - math.log(prices._1)
      (date,returns)
    }
  }
  //.write(Tsv("dateret"))
  .groupAll{
    _.sum[Double]('returns)
  }
  .write(Tsv("totreturn"))
}

