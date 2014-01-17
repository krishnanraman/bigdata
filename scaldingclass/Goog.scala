import com.twitter.scalding._

/* Goals: Find the number of days goog gained, lost & stayed unchanged during 2013
Keywords:
Tsv
read
filter
project
map
groupAll
sum, chaining
* joinWithSmaller, rename
*/


class Goog(args:Args) extends Job(args) {
  val src = Tsv(args("input"), ('Date,'Open, 'High, 'Low, 'Close, 'Volume))
  val sink = Tsv(args("result"))

  // number of days in 2013 goog gained, lost, remained unchanged
  src.read
  .filter('Date) {
    x:String => x.endsWith("-13")
  }
  .project('Open,'Close)
  .map(('Open ,'Close) -> ('gained, 'lost, 'unchanged)) {
    x:(Double,Double) =>
    val (open, close) = x
    (close - open) match {
      case 0 => (0,0,1)
      case a if a > 0 => (1,0,0)
      case b if b < 0 => (0,1,0)
    }
  }
  .project('gained, 'lost, 'unchanged)
  .groupAll {
    group =>
    group.sum[Int]('gained)
    .sum[Int]('lost)
    .sum[Int]('unchanged)
  }.write(sink)
}

/* Task: Modify above program to do the following
1. Find number of days goog & nasdaq simultaneously gained & simulatenously lost in 2008-2013 timeframe
2. replace nasdaq with dow
3. replace nasdaq with s&p
4. As per #1,#2, #3, Goog is most correlated to which index, least to which ?
HINTS:
1. USE pipe1.joinWithSmaller(fields, pipe2)
2. MAKE SURE pipe1 & pipe2 fields do NOT share field names
3. project/discard early
*/
