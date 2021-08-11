package SparkRepo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 4/8/21.
  */
object Assignment2  extends  App{

  val spark = SparkSession.builder().appName("Assignments").master("local").getOrCreate()

  //show all employees and their max salary
  // all employee who where never manager
  //job titles of  best paid 10 employees in the company

  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val usr = "docker"
  val pwd = "docker"

  def readTables (table:String) = {
    spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", url)
      .option("user",usr)
      .option("password",pwd)
      .option("dbtable",s"public.$table")
      .load()
  }

  val employeesDF = readTables("employees")
  val deptmanagerDF = readTables("dept_manager")
  val titlesDF = readTables("titles")
  val salariesDF = readTables("salaries")

  //show all employees and their max salary

  val tempDF = salariesDF.groupBy(col("emp_no")).agg(
                            max(col("salary")).as("max-salary")
                           )

tempDF.show()

  val resDF = employeesDF.join(tempDF,"emp_no").show()

  // all employee who where never manager

  val resDF2 = employeesDF.join(deptmanagerDF,employeesDF.col("emp_no") === deptmanagerDF.col("emp_no") ,"left_anti").show()

  //job titles of  best paid 10 employees in the company

  val mostResentJobTitlesDF = titlesDF.groupBy("emp_no","title").agg(max(col("to_date")))

  val bestPaid10EmpDF = tempDF.orderBy(col("max-salary").desc ).limit(10)

  val bestPaidJobDF = bestPaid10EmpDF.join(mostResentJobTitlesDF,"emp_no").show()

  /* create table table name ()
  clustered by (id) into 5 buckets
  row format delimited
  field terminated by ','
  stored as textfike
  if location givrn -- external table
   */




}
