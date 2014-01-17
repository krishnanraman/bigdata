import com.twitter.scalding._

/* Goals: Find pythagoreans between 1 & 100
Keywords:
Job
Args
IterableSource
Tsv
crossWithSmaller
filter
write
*/

class Pythagoreans(args:Args) extends Job(args) {
  val data = (1 to 100).toList
  val x = IterableSource(data, 'x)
  val y = IterableSource(data, 'y)
  val z = IterableSource(data, 'z)
  val sink = Tsv(args("result"))

  x.crossWithSmaller(y)
  .crossWithSmaller(z)
  .filter('x,'y,'z){
    abc:(Int,Int,Int) =>
    val (a,b,c) = abc
    a*a + b*b == c*c
  }.write(sink)
}

/* Task: Modify above program to do the following
Allocate $10,000 to GOOG, LNKD & TWTR, with ATMOST $10 to spare
GOOG = $1156
LNKD = $230
TWTR = $60
*/
