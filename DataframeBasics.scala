package SparkRepo

/**
  * Created by hduser on 3/6/21.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataframeBasics  extends App{

  val spark = SparkSession.builder()
    .appName("Dataframe Basics")
    .master("local")
    .getOrCreate()

  val firstDF = spark.read
    .format("Json")
    .option("inferSchema","true")
    .load("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.json")


  firstDF.show()

  firstDF.take(10).foreach(println)


  val carSchema = StructType (
    Array(
      StructField("Name",StringType),
      StructField("Miles_per_Gallon",IntegerType),
      StructField("Cylinders",IntegerType),
      StructField("Displacement",IntegerType),
      StructField("Horsepower",IntegerType),
      StructField("Weight_in_lbs",IntegerType),
      StructField("Acceleration",DoubleType),
      StructField("Year",StringType),
      StructField("Origin",StringType)
    )
  )

  val carDFwithSchema = spark.read
                                            .format("Json")
                                            .schema(carSchema)
                                            .load("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.json")

  carDFwithSchema.show()
  carDFwithSchema.printSchema()
}

