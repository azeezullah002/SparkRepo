package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 4/25/21.
  */
object CommonTypes extends  App{

  val spark = SparkSession.builder().appName("Common Types").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema","true").json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")

  //Adding a common plain value

  moviesDF.select(col("Title"),lit(47).as("plain_text"))

  //Booleans
  val dramaFilter = col("Major_Genre") equalTo  "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferedFilter = dramaFilter and goodRatingFilter

  //multiple true false values
  val moviesWithGoodFlag = moviesDF.select(col("Title"),preferedFilter as "Good_movies")

  //or get only good movies (true values)

  moviesWithGoodFlag.where("Good_movies")

  // get only bad movies
  moviesWithGoodFlag.where(not(col("Good_movies"))).show()

  //numbers ((Rotten_tomatoes_rating / 10) + IMDB_ratings) /2

  val moviesAvgRatingDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating")/10 + col("IMDB_Rating"))/2)

  //moviesAvgRatingDF.show()

  //correlations = number between -1 and 1
  //corr is an Action

  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating","IMDB_Rating"))

  //strings

  //capitalization
  //Initcap = It will capitalize every lettle in the given string
  //lower, upper
  //contains

  val carsDF = spark.read.option("inferSchema","True")
    .json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.json")

  carsDF.select(initcap(col("Name")))

  carsDF.select("*").where(col("Name").contains("volkswagen")).show()

  //regex
/* There are 2 most commonly used funtions
1. regexp_extract( colname, regexStr, id) ==> this will filter

2. regexp_replace(colname,regexStr, "string which will be replaced with" ) ==> this will replace

 */

  val regexStr = "volkswagen|vw"

// to pull the cars who have "volkswagen or vw

val vwCars = carsDF.select(col("Name"),
  regexp_extract(col("Name"),regexStr,0)
  .as("Volkswagen_vw")).where(col("Volkswagen_vw").notEqual(""))

  //vwCars.show()


  vwCars.select(col("Name"),
    regexp_replace(col("Name"), regexStr, "Hyndai Car").as("VW to Hyundai")
  )

/*
Exercise
Filter the cars DF with the list of car names obtained via an API call
The API will return a List
 */

  def getCareNames = List("Volkswagen","Ford","Vw","Mercedes-Benz")

  val carRegex = getCareNames.map(_.toLowerCase()).mkString("|");

  println(carRegex);

carsDF.select(col("Name"), regexp_extract(col("Name"), carRegex, 0).as("API Cars")).where(col("API Cars") =!= "").show()


 



}
