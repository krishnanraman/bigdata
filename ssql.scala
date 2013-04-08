import scala.io.Source
import java.io.PrintWriter
import collection.immutable.{IntMap,HashMap}
import collection.mutable.ListBuffer

object ssql {

  val (lp,rp,nl,s2,s4) = ("{", "}", "\n", "  ","    ")

  val scalding = IntMap[String](

  0->"import com.twitter.scalding._",
  1->"class <script>(args : Args) extends Job(args)",
  2->"val <source> = ",
  3-> "TextLine(\"<sourcefile>\")",
  4->".read",
  5->".mapTo('line -> <columns> ){",
  6->"line:String =>",
  7->"val res = line.split(\" \").map( _.trim).filter(_.length > 0)",
  8->".rename(<oldname> -> <newname> )",
  9->".discard(<column>)",
  10->".filter(<column>) { ",
  11->".map((<from>) -> (<to>)) {",
  12->".limit(<count>)",
  13->".write(Tsv(\"<savefile>\"))",
  14->"columns:(<ctypes>)=> ",
  15->"val (<fromcolumns>) = columns"

  )

  val symboltable = HashMap[String,((String,List[String])=>Unit)](

    "open"    -> open _,
    "save"    -> save _,
    "rows"  -> rows _,
    "columns" -> columns _,
    "column"  -> column _,
    "rowsselect" -> rowsselect _,
    "columnadd" -> columnadd _,
    "columnremove" -> columnremove _,
    "columntype" -> columntype _
  )

  var columncount = 3
  val columns = ListBuffer[(Int, String, String)]()
  val sb = new StringBuilder

  val s = scalding

  implicit def mkArray(x:String):Array[Char] = x.toArray

  val a:((Array[Char])=>StringBuilder) = sb.appendAll _

  def r(x:String, y:String) = sb.replace(0,sb.length,sb.replaceAllLiterally(x,y))

  def main(args:Array[String]) = args foreach parser

  def parse(line:String) = {

    val v::s::o =
      line
      .split(" ")
      .toList
      .map(_.trim)
      .filter(_.size>0)

    symboltable( v )(s, o )
  }

  def parser(script:String) = {
    a(s(0))
    a( nl )
    a(s(1))
    r("<script>", script)
    a( lp )
    a( nl )

    Source
    .fromFile(script)
    .getLines
    .toList
    .foreach( line=>parse(line.toLowerCase))

    a( nl )
    a( rp )

    // write the scala code out to screen, file
    val scala = sb.toString
    //println( sb.toString )
    val out = new PrintWriter(script + ".scala")
    out.print( scala )
    out.flush
    out.close
    println("Created " + script + ".scala")
  }

  def save(subject: String, obj: List[String]) ={
    a(s(13))
    r("<savefile>", subject)
  }

  def rows(subject: String, obj: List[String]):Unit = symboltable( "rows"+subject )(subject, obj ) // TBD error handle based on subject

  def rowsselect( subject: String, obj: List[String]) ={
    a(s(10))
    val cname = obj.head
    r("<column>", ""+Symbol(cname))
    val i = columns.indexWhere( x=> x._2 == cname )
    val ctype = mkType(columns(i)._3)
    a(nl); a(s4);
    val str = cname + ":" + ctype + " => "
    a(str)
    a(nl)
    val pred = "(" + obj.mkString(" ") + ")"
    a(s4);a(pred)
    a(nl);a(s2);a(rp);
  }

  def mkType(s:String) = {
    s match {
      case "text" => "String"
      case "number" => "Int"
      case "decimal" => "Double"
    }
  }

  def columntype( subject: String, obj: List[String]) = {
    val row = (subject.toInt, obj.head, obj.tail.head )
    columns += row
    if ( columns.size == columncount ) {

      val mycolumns = columns.sortBy( x=> x._1)
      a(nl);a(s4);
      a(s(5))
      val cnames = mycolumns.map( x=> (Symbol(x._2))).mkString(",")
      r( "<columns>", "(" + cnames + ")")

      a(nl)
      a(s4);a(s(6))
      a(nl)
      a(s4);a(s(7))
      a(nl)
      mycolumns.zipWithIndex.foreach( xi=> {
        val (x,i) = xi
        val myname = x._2
        val mytype = mkType(x._3)
        val str = "val " + myname + ":" + mytype + " = res(" +  i + ").to" + mytype
        a(s4);a(str)
        a(nl)
      })
      val retval = "("+ mycolumns.map( x=> x._2).mkString(",") + ")"
      a(s4); a(retval)
      a(nl)
      a(s2);a(rp)
    }
  }

  def columnadd(subject: String, obj: List[String]) ={
    a(s(11))
    val cname = obj.head
    val rest = obj.tail
    val distinctc = rest.map( c => columns.indexWhere( x => c.contains(x._2))).filter(x => x >= 0).distinct
    val fromc = distinctc.map( i => columns(i)._2).mkString(",")
    val fromcs = distinctc.map( i => Symbol(columns(i)._2)).mkString(",")
    r("<from>", fromcs)
    r("<to>", ""+Symbol(cname))
    val fromtypes = distinctc.map( i => mkType(columns(i)._3)).mkString(",")
    a(nl); a(s4);
    a(s(14))
    r("<ctypes>", fromtypes)
    a(nl);a(s4);
    a(s(15))
    r("<fromcolumns>", fromc)
    a(nl);a(s4);
    a(rest.mkString(" "))
    a(nl);a(s2);a(rp)
  }

  def columnremove(subject: String, obj: List[String]) ={
    a(s(9))
    r("<column>", ""+Symbol(obj.head))
    a(nl);a(s4);
  }

  def columns(subject: String, obj: List[String]) ={
      columncount = subject.toInt
  }

  def column(subject: String, obj: List[String]):Unit = {

    val isInt =
      try {
        subject.toInt
        true
      } catch {
        case e:Exception => false
      }

    symboltable( "column"+ (if( isInt ) "type" else subject ))(subject, obj )
  }

  def open(subject: String, obj: List[String]) = {
    a(s2);a( s(2) )
    r("<source>", subject)
    a(nl)
    a(s4);a( s(3))
    r("<sourcefile>", obj.head)
    a( nl )
    a(s4);a( s(4) )
  }

}
