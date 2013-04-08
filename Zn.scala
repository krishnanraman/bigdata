trait Abelian

case class Zn(order:Int, zero:Int) extends Abelian {
     def identity = zero
     def size = order
     def elements =  (1 to order).toSeq
     def cayley = {
          Vector.tabulate(order,order)((x,y)=> {
               val idx = math.abs(zero - (x+1)) // difference between x & zero
               val timesToShift = if ((x+1)>=zero) idx else (order-idx)
               val row = shift(elements, timesToShift )
               row(y)
          })
     }
     private def shift(s:Seq[Int], pos:Int) = {
          val n = 1+s.indexOf(pos)
          s.slice(n,s.size)++s.slice(0,n)
     }

     def plus(x:Int, y:Int) = cayley(x-1)(y-1)
     def inverse(x:Int) = cayley(x-1).indexOf(zero)+1
}

object Zn {
     def main(args:Array[String]) = {
          val group4 = Zn(4,3)
          println( "Identity of group = " + group4.identity)
          println( "Elements of group = " + group4.elements)
          println( "Cayley table:")
          group4.cayley.foreach( row => println(row.mkString(",")))
          println( "2 + 3 = " + group4.plus(2,3))
     }
}

