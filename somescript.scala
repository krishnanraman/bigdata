import com.twitter.scalding._
class somescript(args : Args) extends Job(args){
  val employees =
    TextLine("file1.txt")
    .read
    .mapTo('line -> ('name,'age,'income) ){
    line:String =>
    val res = line.split(" ").map( _.trim).filter(_.length > 0)
    val name:String = res(0).toString
    val age:Int = res(1).toInt
    val income:Double = res(2).toDouble
    (name,age,income)
  }.filter('age) {
    age:Int =>
    (age > 20)
  }.filter('age) {
    age:Int =>
    (age < 27)
  }.map(('income,'age) -> ('newincome)) {
    columns:(Double,Int)=>
    val (income,age) = columns
    income*1.1 + (age-20)*100
  }.discard('age)
    .discard('income)
    .write(Tsv("file2.txt"))
}
