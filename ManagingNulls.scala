package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 5/1/21.
  */
object ManagingNulls extends  App{

  val spark = SparkSession.builder().appName("Managing Nulls").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema","True").json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")

// select the rating which is not null either rotten tomatoes or imdb ratings

  moviesDF.select(col("Title"), col("Rotten_Tomatoes_Rating"),col("IMDB_Rating"),coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating")*10)).show()

  //checking for nulls

  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  //nulls when ordering desc_nulls_first, desc_null_last

  moviesDF.orderBy(col("IMDB_Rating").asc_nulls_first)

  //na =>special object containing method drop, fill

  //removing nulls- removing rows where the values are null

  moviesDF.select(col("Title"),col("Rotten_Tomatoes_Rating")).na.drop()

  moviesDF.na.drop()

  // replacing nulls with a default value, here "Di

  moviesDF.na.fill(0,List("IMDB_Rating","Rotten_Tomatoes_Rating"))

// replace diff columns with diff values of null using map

  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown",
    "Director" -> "Unknown",
    "Distributor" -> "Chicha"
      ))

//complex operations works only with selectExpr
  //ifnull --> Same as coalesce
  //nvl  --> same as coalesce
  //nullif --> if(1==2) null else 1 ..  returns null if the 2 values equal, else return first
  //nvl2 --> 3 args, if(1 != null) returns 2 else 3

  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10 ) as ifnull",
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10 ) as nvl",
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10 ) as nullif",
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0 ) as nvl2"
  ).show()

}
