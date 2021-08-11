package SparkRepo

/**
  * Created by hduser on 7/22/21.
  */

//Higer Order Functions
  //Take Funtions as Arguments
 // Return Funtions as result

object HigherOrderFunctions {

  def math(x:Double,y:Double, f:(Double,Double) => Double):Double = f(x,y);

  def math2(x:Double,y:Double,z:Double, f:(Double,Double,Double)=>Double):Double = f(x,y,z);

  def main(args: Array[String]): Unit = {
    val result = math(50.0,20.0, (x,y)=>x+y);
    println(result)

    val result2 = math2(50.0,20.0,70, (x,y,z)=>x+y *z);
    println(result2)

  }

}
