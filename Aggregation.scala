package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 4/6/21.
  */
object Aggregation  extends App{

  val spark = SparkSession.builder().appName("Aggregation").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema","true").json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")


  //counting how many diff major_genre we have in our json

  moviesDF.show()

  val genreCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values that are not null
  genreCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)").show()

  //count all
  moviesDF.select(count("*")).show()

    //count distinct

  moviesDF.select(countDistinct(col("Major_Genre")) as "DistinctCnt").show()

// appoximate count

  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

// min & max

  moviesDF.selectExpr("min(IMDB_rating) as min").show()
  moviesDF.select(max(col("IMDB_rating")) as "max").show()

  //sum
  moviesDF.select(sum(col("US_Gross")) as "Sum").show()

//avg
  moviesDF.select(avg(col("US_Gross")) as "Avg").show()

  //mean & stdDeviation
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")) as "Mean" ,
    stddev(col("Rotten_Tomatoes_Rating")) as "Std Dev"
  ).show()


  //grouping // select count(*) from table group by "Major_genre";

  moviesDF.groupBy("Major_Genre").count().show()

  moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating").show()

  //agg method-- most powerful for aggregation

  moviesDF.groupBy("Major_Genre").agg(
  count("*") as "TotalGenre",
    avg("IMDB_Rating") as "Avg_Rating"
  ).orderBy(col("Avg_Rating")).show()

  /* Exercise
  Sum up all the profits of all the movies
  count of how many distint directors we have
  show the mean n std dev of US gross for the movie
  compute the avg IMDB rating and avg us gross revenue per Director
   */

  moviesDF.select(
    sum(col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")) as "Total_Profit"
  ).show()

 // moviesDF.groupBy("Director").count().show()

  moviesDF.select(countDistinct(col("Director"))).show()



  moviesDF.agg(
    mean(col("US_Gross")) as "Mean",
    stddev(col("US_Gross")) as "StdDev"
  ).show()

  moviesDF.groupBy("Director").agg(
    avg(col("IMDB_Rating")) as "AverageIMDBRating",
    avg(col("US_Gross")) as "AverageUSGross"
  ).orderBy(col("AverageIMDBRating").desc_nulls_last).show()

}
