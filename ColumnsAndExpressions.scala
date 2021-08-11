package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * Created by hduser on 4/6/21.
  */
object ColumnsAndExpressions  extends  App{
val spark = SparkSession.builder().appName("Projection")
    .master("local").getOrCreate()

  val carDF = spark.read.option("inferSchema","true")
    .json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.json")

  carDF.show()


  //columns
val firstColumn = carDF.col("Name") // the result will be a column object n not df and must be used with select

 //selecting projecting

  val carNames = carDF.select(firstColumn).show()

  //various selecting methods

  //for using ' & expr we need to import spark implicits

  import spark.implicits._

  carDF.select(
    carDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year,
    $"Horsepower", // fancier interpolated string
    expr("Origin") // Expression
      )


  carDF.select("Name","Origin")

  //expressions
  val simpExpr = carDF.col("Weight_in_lbs")

  val weightInKgs = carDF.col("Weight_in_lbs") / 2.2

  val carswithweight = carDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgs.as("Weight_in_kgs")
  ).show()

  //selectExpr
  val carsWithSelectExpr = carDF.selectExpr("Name", "Weight_in_lbs","Weight_in_lbs / 2.2").show()

  //adding a new column

  val newCarDF = carDF.withColumn("Weight_in_kg", col("Weight_in_lbs")/2.2)

  newCarDF.show()

  //rename an existing column

  val carsWithColumnRename = carDF.withColumnRenamed("Weight_in_lbs" , "Weight_in_pounds")

  //remove a column

  carsWithColumnRename.drop("Cylinder")

  //filtering

  val europeanDF = carDF.filter(col("Origin") =!= "USA")
  val europeanDF2 = carDF.where("Origin != 'USA'")

  val americanDF = carDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanDF2 = carDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))

  val americanDF3 = carDF.filter("Origin = 'USA' AND Horsepower >  150")

  //unioning = adding more rows

  val moreCars = spark.read.option("inferSchema","true")
    .json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/more_cars.json")

  val unionDF = carDF.union(moreCars)

  //distinct values

  val allCountryDF = carDF.select("Origin").distinct()

  allCountryDF.show()


  /*
  Read movies DF n select 2 columns

  create newDF by summing up 3 columns as total profit
   us_gross + world_wide_gross + dvd_sales

   select all the comedy movies in major_ with imdb rating above 6

   */



}
