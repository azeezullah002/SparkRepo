package SparkRepo

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by hduser on 4/3/21.
  */
object Assignment  extends App {

  /*create a manual DF => SmartPhones
  -make
  -model
  -platform
  */

  val spark = SparkSession.builder().appName("Assignments").master("local").getOrCreate()

  val smartPhone = Seq(
    ("Samsung", "Galaxy s10", "Android", 12),
    ("Apple", "Iphone", "iOS", 14),
    ("Nokia", "Nokia 6600", "Symbian", 12)
  )

  import spark.implicits._

  val smartPhoneDF = smartPhone.toDF("Make", "Model", "Platform", "Megapixel")

  smartPhoneDF.show();


  val moviesDF = spark.read.format("Json").option("inferSchema","true")
                                .load("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")

  moviesDF.show()
  println(s"The MoviesDF has  ${moviesDF.count()} rows")


  //Read the movies DF then write is as
  //tab sep value files
  //snappy parquet
  // tablee public.movies in progress DB


  val moviesDFNew = spark.read.format("Json").option("inferSchema","true")
    .load("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")

  moviesDFNew.write.format("csv")
      .option("sep","\t")
        .option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.csv")

  moviesDFNew.write.mode(SaveMode.Overwrite).save("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies-parquet")

  moviesDFNew.write
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
      .mode(SaveMode.Overwrite)
    .save()



  /*
  Read movies DF n select 2 columns

  create newDF by summing up 3 columns as total profit
   us_gross + world_wide_gross + dvd_sales

   select all the comedy movies in Major_Genre with imdb rating above 6

   */

  val movieDF = spark.read.option("inferSchema","true")
    .json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")


  moviesDF.show()

  val selectedMoviesDF = movieDF.select(col("Title"), 'Release_date,$"Major_Genre",expr("IMDB_Rating")).show()

  val totalProfitDF = movieDF.select(col("US_Gross"),'US_DVD_Sales,expr("Worldwide_Gross"),
    ('US_Gross  + 'Worldwide_Gross ).as("Total_Profit"))

  totalProfitDF.show()

  val totalProfitDF2 = movieDF.selectExpr("Title",
    "US_Gross","Worldwide_Gross",  "US_Gross + Worldwide_Gross as TotalProfit"
  )
  totalProfitDF2.show()

  val totalProfit3 = movieDF.select("Title", "US_Gross","Worldwide_Gross").withColumn("TotalProfit", col("US_Gross") + col("Worldwide_Gross"))

  totalProfit3.show()

  val goodComedyDF = movieDF.filter("Major_Genre = 'Comedy' AND IMDB_Rating > 6").select("Title","IMDB_Rating","Major_Genre")

  goodComedyDF.show()


  val goodComedyDF2 = movieDF.where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).select("Title","IMDB_Rating","Major_Genre")

  goodComedyDF2.show()

}