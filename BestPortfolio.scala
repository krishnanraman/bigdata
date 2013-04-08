import cern.colt.matrix.{DoubleFactory2D, DoubleFactory1D }
import cern.colt.matrix.linalg.Algebra
import java.util.StringTokenizer

object BestPortfolio {
    def main(args:Array[String]) = {
      val alg = Algebra.DEFAULT

      // define the correlation matrix
      val data = Array(Array(0.448, 0.177, 0.0, 0.017),
        Array(0.177, 0.393, 0.177, 0.237),
        Array(0.0, 0.177, 0.237, 0.06),
        Array(0.017, 0.237, 0.06, 0.19))
      val corr = DoubleFactory2D.dense.make(data)
      val file = scala.io.Source.fromFile("invest.txt").getLines.toList

      // convert the tab-delimited weights in the file to a row vector
      def getWeights(s:String) = {
           val weights = s.split("\t").map( x=> x.toDouble)
           DoubleFactory1D.dense.make(weights)
      }
      // compute risks per tuple and sort by risk
      file.map(line=> {
           val w = getWeights(line)
           ( line, alg.mult( alg.mult(corr,w), w))
      }).sortBy(x=>x._2)
      .foreach( println )
  }
}
