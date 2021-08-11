package SparkRepo

import org.apache.spark.sql.SparkSession

/**
  * Created by hduser on 4/7/21.
  */
object Joins extends  App{

  val spark = SparkSession.builder().appName("Joins").master("local").getOrCreate()

  val bandsDF = spark.read.option("inferSchema","true").json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/bands.json")
  val guitarsDF = spark.read.option("inferSchema","true").json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/guitars.json")
  val guitarPlayersDF = spark.read.option("inferSchema","true").json("/home/hduser/Projects/spark-essentials-master/src/main/resources/data/guitarPlayers.json")


//joins
val joinCnd = guitarPlayersDF.col("band") === bandsDF.col("id")

  //inner
  val guitaristBandsDF = guitarPlayersDF.join(bandsDF,joinCnd,"inner").show()


  //left outer

  guitarPlayersDF.join(bandsDF,joinCnd,"left_outer")

//right outer

  guitarPlayersDF.join(bandsDF,joinCnd,"right_outer")

  //outer
  guitarPlayersDF.join(bandsDF,joinCnd,"outer")

  // semi joins : only see the columns in left df matching the right df
  // ( you will wont see right df columns )


  guitarPlayersDF.join(bandsDF,joinCnd,"left_semi").show()

  /*
 +----+-------+---+------------+
|band|guitars| id|        name|
+----+-------+---+------------+
|   0|    [0]|  0|  Jimmy Page|
|   1|    [1]|  1| Angus Young|
|   3|    [3]|  3|Kirk Hammett|
+----+-------+---+------------+

  */

  bandsDF.join(guitarPlayersDF,joinCnd,"left_semi").show()

  /*
  +-----------+---+------------+----+
|   hometown| id|        name|year|
+-----------+---+------------+----+
|     Sydney|  1|       AC/DC|1973|
|     London|  0|Led Zeppelin|1968|
|Los Angeles|  3|   Metallica|1981|
+-----------+---+------------+----+
   */

  //anti-joins : which will only keep the rows from left DF for which there is no match in the right df

  guitarPlayersDF.join(bandsDF,joinCnd,"left_anti").show()
  /*
  +----+-------+---+------------+
|band|guitars| id|        name|
+----+-------+---+------------+
|   2| [1, 5]|  2|Eric Clapton|
+----+-------+---+------------+
   */

  
}
