import com.twitter.scalding.{Dsl,Source,TextLine,Job,Args,Tsv,RichPipe}
import cascading.pipe.Pipe

case class USPopulationSource(override val p:String) extends TextLine(p) {
  override def transformForRead(pipe : Pipe) = {
    import Dsl._
    RichPipe(pipe).mapTo('line->('year,'state,'fips,'isWhite,'isBlack, 'isHispanic,'isMale,'age,'population)) {
      record:String =>
      val year:Int             = record.slice(0,0+4).toInt
      val state:String         = record.slice(4,4+2)
      val fips:String          = record.slice(6,6+5)
      val isWhite:Boolean      = record.slice(13,13+1).toInt == 1
      val isBlack:Boolean      = record.slice(13,13+1).toInt == 2
      val isHispanic:Boolean   = record.slice(14,14+1).toInt == 1
      val isMale:Boolean       = record.slice(15,15+1).toInt == 1
      val age:Int              = 5*(record.slice(16,16+2).toInt -1)
      val population:Int       = record.slice(18,18+8).toInt

      (year,state,fips,isWhite,isBlack, isHispanic,isMale,age,population)
    }
  }
}

class PopulationStats(args:Args) extends Job(args) {

  // simple population stats
  /*
  2008  ALL   2909898989
  2008  Female  154604015
  2008  Male  149489951
  2009  ALL   306755435479
  2009  Female  155964075
  2009  Male  150807454
  */
def popstats(people:Pipe) = {

  // first get male, female per year
  val pipe1 = people.map('isMale->'sex) { isMale:Boolean =>
    if( isMale) "Male" else "Female"
  }.groupBy('year, 'sex) {
    group => group.plus[Int]('population->'population)
  }

  // now get ALL
  val pipe2 = pipe1.groupBy('year) {
    group => group.plus[Int]('population->'population)
  }.map(('year,'population)->('year,'sex,'population)){
    x:(Int,Int) =>
    (x._1,"ALL",x._2)
  }

  // now combine ALL with male, female
  val pipe3 = (pipe1++pipe2).
  groupAll{ _.sortBy('year,'sex)}.
  write(Tsv("pop_stats_3.txt"))

}


  // absolute growth rate
  def absGrowthRate(people:Pipe, fipspipe:Pipe) = {
    people.groupBy('year, 'fips){
      group => group.plus[Int]('population->'population)
  }.groupBy('fips) {
     val init = (0,0.0d)
     type X = (Int,Double)
     type T = (Int,Int)

     // foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
      group => group.foldLeft[X,T]( ('population,'year) -> ('dummy,'growth))(init:X) {

        (x:X, t:T) =>
          val (population,year) = t
          val (dummy, growth ) = x
          year match {
          case 1969 => (population, 0.0d)
          case 2011 => (population,(population-dummy)/(dummy+0.0d))
          case _ => if (dummy==0) (population,0.0d) else (dummy,0.0d)
      }
    }
  }.project('fips,'growth)
  .joinWithSmaller(('fips-> 'fips), fipspipe)
  .project('state,'county,'growth)
  .groupAll(_.sortBy('growth))
  .write(Tsv("absgrowth.txt"))
  }

  // fine grained growth rate starting from base year
  def fineGrained(baseYear:Int, people:Pipe, fipspipe:Pipe) = {
      val pipe = people.groupBy('year, 'fips){
        group => group.plus[Int]('population->'population)
      }.groupBy('fips) {

       val init = (0,List(0.0d))
       type X = (Int,List[Double])
       type T = (Int,Int)

       // foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
        group => group.foldLeft[X,T]( ('population,'year) -> ('prev,'growth))(init:X) {

          (x:X, t:T) =>
            val (population,year) = t
            val (prev, growth ) = x

            year match {
            case year if (year == baseYear ) => (population, List(0.0d))
            case _ => {
              if( prev != 0) (population,(population-prev)/(prev+0.0d)::growth)
              else (population,List(0.0d))
            }
          }
        }
      }.project('fips,'growth)
      .map('growth->('upyears,'avg)){
        growth:List[Double] => {
          val upyears = growth.map(x=>if (x>0.0d) 1 else 0).sum
          val nonzeros = growth.filter(x=>x!=0.0)
          val average_rate = nonzeros.sum/(nonzeros.size+0.0d)
          (upyears,average_rate)
        }
      }.joinWithSmaller(('fips-> 'fips), fipspipe)
      .project('state,'county,'upyears,'avg)

      pipe.groupAll(_.sortBy('upyears,'avg))
      .write(Tsv("steadygrowth"+baseYear+".txt"))

      pipe.groupAll(_.sortBy('avg))
      .write(Tsv("averagegrowth"+baseYear+".txt"))
  }

  def sexratio(baseYear:Int, agerange:(Int,Int), people:Pipe, fipspipe:Pipe) = {
    people.filter('year,'age) {
    yearage:(Int,Int) =>
    val (year,age) = yearage
    (year == baseYear) && (age>=agerange._1) && (age<=agerange._2) // marriageable age
  }.groupBy('fips, 'isMale){
      group => group.plus[Int]('population->'population)
  }.groupBy('fips) {
    val init = 0.0d
    type X = Double
    type T = (Int,Boolean)
    // foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
    group => group.foldLeft[X,T]( ('population,'isMale) -> ('sexratio))(init:X) {
       (x:X, t:T) =>
       val ratio = x
       val (population,isMale) = t
       if(ratio==0.0) (population+0.0d)
       else {
        val sexratio = ratio/(population+0.0d)
        if (isMale) 1.0/sexratio else sexratio
       }
    }
  }.joinWithSmaller(('fips-> 'fips), fipspipe)
  .project('sexratio, 'state,'county)
  .groupAll(_.sortBy('sexratio))
  .write(Tsv("sexratio"+baseYear+".txt"))
  }

  def race(baseYear:Int, agerange:(Int,Int), people:Pipe, fipspipe:Pipe, white:Boolean) = {
    val filename = (if (white) "white" else "black")+baseYear+".txt"

    // first get total population ( ie. all races ) of agegroup for baseYear
    val pipe1 = people.filter('year,'age){
      yearage:(Int,Int) =>
      val (year,age) = yearage
      (year == baseYear) && (age>=agerange._1) && (age<=agerange._2)
    }.groupBy('isMale,'fips){
      group => group.plus[Int]('population->'population) // add (fe)male blacks, (fe)male hispanics, (fe)male whites etc.
    }.groupBy('fips){
      group => group.plus[Int]('population->'population) // add males + females
    }

    // now count people of certain race in certain age group
    val pipe2 = people.filter('year,'age, 'isWhite,'isBlack) {
    yearagewb:(Int,Int,Boolean,Boolean) =>
    val (year,age,isWhite,isBlack) = yearagewb
    (year == baseYear) && (age>=agerange._1) && (age<=agerange._2) && (if (white) isWhite else isBlack)
    }.groupBy('isMale,'fips){
      group => group.plus[Int]('population->'population) // add (fe)male blacks, (fe)male hispanics, (fe)male whites etc.
    }.groupBy('fips){
      group => group.plus[Int]('population->'mypop) // add males + females
    }

    // now compute ratio & sort by ratio
    val pipe3 = pipe1.joinWithSmaller(('fips-> 'fips), pipe2)
    .map( ('mypop, 'population)->'raceratio){
      mypop_total:(Int,Int) =>
      val( mypop, population) = mypop_total
      mypop/(population+0.0d)
    }
    .project('population, 'raceratio,'fips)

    // now join with verbose fips
    pipe3.joinWithSmaller(('fips-> 'fips), fipspipe)
    .project('raceratio,'population, 'state,'county)
    .groupAll(_.sortBy('raceratio))
    .write(Tsv(filename))

  }


  // read inputs
  val people = USPopulationSource(args("pop")).read
  val fipspipe = TextLine(args("fips")).read.mapTo('line->('state,'county,'fips)) {
    line:String =>
    var arr = line.split(",")
    (arr(0),arr(1),(arr(2)+arr(3)))
  }

  /* Want: sample output
  2008  ALL   2909898989
  2008  Female  154604015
  2008  Male  149489951
  2009  ALL   306755435479
  2009  Female  155964075
  2009  Male  150807454
  */
  popstats(people)

  /* Want: absolute fastest growing county in the US
  */
  absGrowthRate(people,fipspipe)

  /* Want: fine-grained fastest steady growing county in the US
  */
  fineGrained(1969,people,fipspipe)
  fineGrained(2001,people,fipspipe) // post dot-com bust

  /*
     Want : sex ratio in 2011 of youth
  */
  sexratio(2011,(20,40),people,fipspipe)

  /*
    Want: white people (white=true) in an agegroup
  */
  race(2011,(20,40),people,fipspipe,true)

  /*
    Want: non-white ( white=false) people in an agegroup
  */
  race(2011,(20,40),people,fipspipe,false)



  //racerelations ?? ratio of whites to blacks, diversity
}
