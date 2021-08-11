package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 7/16/21.
  */
object TaxiApplication {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Taxi Application").master("local").getOrCreate()

    val taxiDF = spark.read.load("/home/hduser/Downloads/spark-essentials-master/src/main/resources/data/yellow_taxi_jan_25_2018")

    //taxiDF.printSchema()
    // println(taxiDF.count());
    //taxiDF.show()

    val taxiZonesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .csv("/home/hduser/Downloads/spark-essentials-master/src/main/resources/data/taxi_zones.csv")

    //taxiZonesDF.show()

    /*
  The questions
  1. Which zones have the most pickups / dropoffs overall?
  2. What are the Peak hours for taxi
  3. How are the trips distributed? Why are people taking the cab?
  4. What are the peak hours for long and short trips?
  5. What are the top 3 pickup/dropoff zones for long / short trips?
  6. How are people paying for the ride, on long/short trips?
  7. How is the payment type evolving over time?
  8. Can we explore a ride-sharing opportunity by grouping close short trips?
   */


    val pickupsByTaxiZones = taxiDF.groupBy("PULocationID")
      .agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .drop("LocationID", "service_zone")
      .orderBy(col("totalTrips").desc_nulls_last)

    //pickupsByTaxiZones.show()

    val pickupsByBorough = pickupsByTaxiZones.groupBy("Borough")
      .agg(sum("totalTrips").as("totalpickups"))
      .orderBy(col("totalpickups").desc_nulls_last)
    //pickupsByBorough.show()


    //  2. What are the Peak hours for taxi

    val peakHours = taxiDF.withColumn("hourOfDay", hour(col("tpep_pickup_datetime")))
      .groupBy("hourOfDay").agg(count("*").as("totalTrips")).orderBy(col("totalTrips").desc_nulls_last)

    //peakHours.show()

    //3. How are the trips distributed? Why are people taking the cab?
    // long and short trips

    val longDistanceThreshold = 30

    val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
    val tripsDistributedDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
      .groupBy(col("isLong")).count()
    //tripsDistributedDF.show()

    // 4. What are the peak hours for long and short trips?

    val peakupsByHoursByLength = tripsWithLengthDF.withColumn("hourOfDay", hour(col("tpep_pickup_datetime")))
      .groupBy("hourOfDay", "isLong").agg(count("*").as("totalTrips")).orderBy(col("totalTrips").desc_nulls_last)

    //peakupsByHoursByLength.show(48)

    // 5. What are the top 3 pickup/dropoff zones for long / short trips?
    //short
    val pickUpDropOffPopularityDFS = tripsWithLengthDF.where(not(col("isLong")))
      .groupBy("PULocationID", "DOLocationID")
      .agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "PickUpZone")
      .drop("LocationID", "Borough", "service_zone")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "DropUpZone")
      .drop("LocationID", "Borough", "service_zone")
      .orderBy(col("totalTrips").desc_nulls_last)


    //long
    val pickUpDropOffPopularityDFL = tripsWithLengthDF.where(col("isLong"))
      .groupBy("PULocationID", "DOLocationID")
      .agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "PickUpZone")
      .drop("LocationID", "Borough", "service_zone")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "DropUpZone")
      .drop("LocationID", "Borough", "service_zone")
      .orderBy(col("totalTrips").desc_nulls_last)

    ///pickUpDropOffPopularityDFS.show///

    //6. How are people paying for the ride, on long/short trips?


    val rateCodeDistributionDF = taxiDF.groupBy("RatecodeID").agg(count("*").as("totalTrips")).orderBy(col("RatecodeID").desc_nulls_last)

    //rateCodeDistributionDF.show()

    //7. How is the payment type evolving over time?

    val rateCodeEvolution = taxiDF.groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
      .agg(count("*").as("totalTrips"))
      .orderBy(col("pickup_day"))

    rateCodeEvolution.show()


  }
}