import com.twitter.algebird.{CountMinSketchMonoid, CMS}
object CMSTest {
  // sane CMS builder
  def mkCMS( probability:Byte = 90, EPS:Double = 0.01 ) = {
    assert( probability >0 && probability < 100 )
    val DELTA = 1 - probability/100.0
    val SEED = 1
    new CountMinSketchMonoid(EPS, DELTA, SEED)
  }

  def updateCMS( monoid:CountMinSketchMonoid, cms:Option[CMS], x:Long ):Option[CMS] = {
    val item = monoid.create(x)
    if( cms.isDefined )  Some(item ++ cms.get) else Some(item)
  }

  def main(args:Array[String]) = {

      // make items from 1 to n, assume k has been seen n-k times
      val n = 100
      val stream = List.tabulate[List[Long]](n)( k => {
        val freq = n - k
        List.fill[Long](freq)( k )
      }).flatten

      // assume this data comes in one at a time from some stream
      // update the CMS as the data arrives
      val monoid = mkCMS( args(0).toByte, args(1).toDouble )
      val cmsitem:Option[CMS] = None
      val result = stream.foldLeft(monoid, cmsitem)( (a,b) => {
        val (mymonoid, myitem) = a
        val number = b
        val newitem = updateCMS( mymonoid, myitem, number )
        val retval = (mymonoid, newitem)
        retval
      })._2.get

      // compare estimated frequencies vs actuals
      val estimate = result.frequency ( 34 ).estimate
      val actual = n - 34
      printf( "Estimate: %d, Actual : %d\n", estimate, actual)
  }
}
