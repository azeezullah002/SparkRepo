package SparkRepo

/**
  * Created by hduser on 7/22/21.
 1. Lists - immutable by default

  */

object Collections {
  def main(args: Array[String]): Unit = {
      val year1 = List("Jan","feb","mar")
      val year2 = List("apr","may","jun  ")

    //combine 2 lists with 2 ways
    // ::: and ++

    val res = year1 ::: year2
    val res2 = year1 ++  year2


    println(res)
    println(res2)

    println(res.size)
    println(res.head)
    println(res.tail)

     for (months <- res){
       println(months)
     }

    val lst = List.range(1,10)
    println(lst)

    var sum = 0
    println(lst.foreach(sum+= _ ))

  }

}
