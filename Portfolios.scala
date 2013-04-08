import com.twitter.scalding._
import com.twitter.scalding.mathematics.Combinatorics

class Portfolios(args : Args) extends Job(args) {

    val cash = 1000.0 // money at hand
    val error = 1 // its ok if we cannot invest the last dollar
    val (kr,abt,dltr,mnst) = (27.0,64.0,41.0,52.0) // share prices
    val stocks = IndexedSeq( kr,abt,dltr,mnst)

    Combinatorics.weightedSum( stocks, cash,error).write( Tsv("invest.txt"))
}


