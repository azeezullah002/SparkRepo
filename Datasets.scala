package SparkRepo

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 5/2/21.
  */

/*
Datasets are distributed collection of jvm objects
All Dataframes are rows of datasets

 */
object Datasets  extends App{

  val spark = SparkSession.builder().appName("DataSets").master("local").getOrCreate()



  /* Here are the steps involved for Data sets

  1. Define a case class
  2. Read dataframe from the file
  3. Define an encoder importing the implecits
  4. Convert DF to DS

  */

  case class Car(
                Name : String,
                Miles_per_Gallon: Option[Double],
                Horsepower: Option[Long],
                Displacement:Double,
                Cylinders:Long,
                Acceleration:Double,
                Origin:String,
                Weight_in_lbs:Long,
                Year:Date
                )


  def readDF(fileName : String) = spark.read.option("inferSchema","true")
    .json(s"/home/hduser/Projects/spark-essentials-master/src/main/resources/data/$fileName")

  import spark.implicits._

  val carsDF = readDF("cars.json")

val carsDS = carsDF.as[Car]

  // we can apply map flapmap reduce on datasets, folds

  //names to upper case

  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  carNamesDS.show()

  // Nullpointer Exception occured as we have nulls in our dataset for multiple columns
//Caused by: java.lang.NullPointerException: Null value appeared in non-nullable field:
  // so we need to a handle nulls above case class using option Miles_per_Gallon


  /* Excercise
  1. count how many cars we have
  2. how many cars with horse power > 140
  3. Average horse power for entire DS

   */

  println(carsDS.count())

  //since   Horsepower has null values, we have to use getorElse
  println(carsDS.filter(x=>x.Horsepower.getOrElse(0L) > 140).count())

  println(carsDS.map(x=>x.Horsepower.getOrElse(0L)).reduce(_+_)/carsDS.count())

  carsDS.select(avg("Horsepower")).show()


  //joins
//when we used id:Int , we received this error because of Inferschema hence changed to Long
  /*
  Exception in thread "main" org.apache.spark.sql.AnalysisException: Cannot up cast `band` from bigint to int as it may truncate
    The type path of the target object is:
    - field (class: "scala.Int", name: "band")
  - root class: "practice.Datasets.GuitarPlayer"
  *
  */

  case class Guitar(Id:Long,make:String,model:String,guitarType:String)
  case class GuitarPlayer(Id:Long,name:String,guitars: Seq[Long],band:Long)
  case class Band(Id:Long,name:String,hometown:String,year:Long)

  val guitarDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandDS = readDF("bands.json").as[Band]

  val guitarBandPlayersDS = guitarPlayerDS.joinWith(bandDS,guitarPlayerDS.col("band")=== bandDS.col("Id"),"inner")

  guitarBandPlayersDS.show()

/* Exercise
join guitersDF and guitarPlayerDS in outer
user array_contains
since where are comparing array with single value, we use array contains
 */

  val guitarPlayersDS = guitarPlayerDS
    .joinWith(guitarDS, array_contains(guitarPlayerDS.col("guitars"), guitarDS.col("Id")), "outer")

  guitarPlayersDS.show()

  //Grouping
//group the cars by origin

  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count().show()
}
