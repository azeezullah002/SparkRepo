package SparkRepo

import org.apache.spark.sql.SparkSession

/**
  * Created by hduser on 8/5/21.
  */
object HiveSparkConnect {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Connection to Hive")
      .config("spark.master", "local")
      //.config("spark.sql.hive.convertMetastoreParquet", false)
      .config("spark.submit.deployMode", "client")
      //.config("spark.jars", "target/")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()





        //import spark.implicits._

        spark.sql("use training")

        spark.sql("select * from sales1").show(40)



    /*
            spark.sql("create table hivetab (name string, age int, location string) row format delimited fields terminated by ',' stored as textfile")
            spark.sql("load data local inpath '/home/hduser/hive_practise/emp' into table hivetab").show()
            val x = spark.sql("select * from hivetab")
            x.write.saveAsTable("hivetab")

             */

  }

}
