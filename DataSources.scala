package SparkRepo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by hduser on 4/3/21.
  */
object DataSources  extends  App{

  val spark = SparkSession.builder().appName("Datasource").master("local").getOrCreate()

  /*while reading
      mode specifies what spark has to do incase it encounters malform record
      failFast, permissive(default), dropMalformed
   */

  val carSchema = StructType (
    Array(
      StructField("Name",StringType),
      StructField("Miles_per_Gallon",IntegerType),
      StructField("Cylinders",IntegerType),
      StructField("Displacement",IntegerType),
      StructField("Horsepower",IntegerType),
      StructField("Weight_in_lbs",IntegerType),
      StructField("Acceleration",DoubleType),
      StructField("Year",DateType),
      StructField("Origin",StringType)
    )
  )

  val carDFwithSchema = spark.read
    .format("Json")
      .options(
              Map("mode" -> "failFast",
                  "path" -> "/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.json",
                  "inferSchema" -> "true"
              )
          ).load()


  /*writing dfs
  -format
  -save mode = overwrite,append,ignore,errorifExist
  -path

  */

    carDFwithSchema.write
                              .format("json")
                              .mode(SaveMode.Append)
                              .option("path","/home/hduser/Projects/spark-essentials-master/src/main/resources/data/newWrite.json")
                                .save()


/* Date Format

Date format will only work if we specify a schema
Above schema has year with DateType
if parsing fail, it will put null


 */

  val carDF = spark.read.format("Json")
      .schema(carSchema)
    .option("dateFormat","YYYY-MM-dd")
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed") //bzip2,gziplz4,snappy,deflate
    .load("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.json")

  carDF.show()
  carDF.printSchema()


    //csv flags

  val csvSchema = StructType(Array(
        StructField("symbol",StringType),
        StructField("date",DateType),
        StructField("price",DoubleType)
  ))
  val csvDF = spark.read
    .schema(csvSchema)
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .csv("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/stocks.csv")

  //parquet

  csvDF.show()

  csvDF.write.mode(SaveMode.Overwrite)
    .save("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/cars.parquet")

  //text file

  spark.read.text("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/example.txt").show()

  //Reading from remote database

  val employeesDF = spark.read
              .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
  .option("dbtable","public.employees")
    .load()

  employeesDF.show()



}
