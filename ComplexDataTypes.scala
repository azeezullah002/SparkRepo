package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 5/1/21.
  */
object ComplexDataTypes  extends  App {
  val spark = SparkSession.builder().appName("Complex Types").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/movies.json")


  /* we have seen how to handle date format using schema
  if the date is in string type then
   */
  //to_date does conversion
  // current_date
  //current_timestamp
  //datediff  calculates the date difference between 2 days in to no of days

  val moviesWithReleaseDate = moviesDF.select(col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("CurrentTime", current_timestamp())
    .withColumn("Movie Age", datediff(col("Today"), col("Actual_Release")) / 365)

  // there are some of the dates in movies json for col release date which are not of format "dd-MMM-yy"
  //spark to_date will not crash but instead return null

  moviesWithReleaseDate.select("*").where(col("Actual_Release").isNull)

  /* Exercise \
  1. How to deal with multiple date formats
  2. Read the stocks df and parse the dates

   */
  /*
1. Parse the DF multiple times, then union the small dfs
 */

  val stockDF = spark.read.option("inferSchema", "True").option("header", "True")
    .csv("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/stocks.csv")

  val stockDFWithDates = stockDF.withColumn("Actual_Date", to_date(col("date"),"MMM dd yyyy"))

//  stockDFWithDates.show()

  //structures : Groups of colunms aggregated into one

  /*
|  o Kill A Mocking...|[13129846, 13129846]|
|    Tora, Tora, Tora  | [29548291, 29548291]|
|   Hollywood Shuffle|  [5228617, 5228617]|

use getField() to get a field
   */

  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("profit"))
    .select(col("Title"),col("profit").getField("US_Gross").as("US Profit"))


  // other way to get, with expressions

  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
                    .selectExpr("Title","Profit.US_Gross")

  //Arrays
  // spliting the title based on space or comma using split method

  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title")," |,").as("Title_words"))

  moviesWithWords.select(
    col("Title"),
    expr("Title_words[0]"),
    size(col("Title_words")),
    array_contains(col("Title_words"),"Love")
  ).show()

}