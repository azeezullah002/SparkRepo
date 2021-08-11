package SparkRepo

import scala.collection.mutable.Stack
object ScalaLearning {

  def main(args: Array[String]): Unit = {
    val AList = List("customers-1.0.23","products-2.8.1","products-2.7.2","customers-10.2.3")
    val temp = AList.toArray
    var p = Stack[String]()
    var c = Stack[String]()


    for (i <- temp)
    {
      var tempz = i.charAt(0)

      if(tempz == 'c')
      {

        var Ctmp = i.split("-");
        //println(findBigger())
        c.push(Ctmp(1))
      }else{
        var Ptmp = i.split("-");
        p.push(Ptmp(1))
      }

      i+1
    }
    c.foreach(println)
    findBigger(c(0),c(1))
   // c.foreach(println)

   // p.foreach(println)

  }

  def findBigger(x:String,y:String) : Unit ={



    println("aaaaaaaaaaa")

    val a =x.split(".").map(_.trim)
    val b = y.split(".").map(_.trim)
    println(a)

     a.foreach(println)
    b.foreach(println)



/*
    if(a(0) > b(0))
    x
    else if(a(1) > b(1))
    x
    else if(a(2) > b(2))
    x
    else if (b(0) > a(0))
    y
    else if (b(1) > a(1))
    y
    else if (b(2) > a(2))
    y
    */
  }
}
