package jdbcspark
// import Spark libraries

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.Instant
import scala.util.Try

final case class User(
    id: Int,
    lastName: String,
    firstName: String,
    age: Int,
    numFriends: Int,
    date_created: Timestamp
)

trait ParseArgCommand {}
// Init command
case class InitCommand() extends ParseArgCommand
// Fill command
case class FillCommand(numUsers: Int) extends ParseArgCommand
// Help command
case class HelpCommand() extends ParseArgCommand

object SimpleSparkApp {

  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  val jdbcUser = "postgres"
  val jdbcPassword = "postgres"

  def main(args: Array[String]): Unit = {

    // Analyse the arguments
    //
    // If arg --init is present, then use DatabaseHelper to create the database and the table
    // If arg --fill nb users, then use DatabaseHelper to fill the database with fake data for nb users
    // If arg --help is present, then print the help message

    // parse the arguments
    val argList = args.toList

    // Number of users to fill the database with
    var numFillUsers: Option[Int] = None

    // check if there are no arguments
    val noArguments = argList.isEmpty


    var fillCommand : Option[FillCommand] = None    
    var initCommand : Option[InitCommand] = None
    var helpCommand : Option[HelpCommand] = None

    for (arg <- argList) {
      arg match {
        case "--init" =>
          initCommand = Some(InitCommand())
        case "--fill" =>
          val numFillUsers: Int = Try(argList(argList.indexOf(arg) + 1)) match {
            case scala.util.Success(value)     => value.toInt
            case scala.util.Failure(exception) => 100000
          }
          fillCommand = Some(FillCommand(numFillUsers))
        case "--help" =>
          helpCommand = Some(HelpCommand())
        case _        =>
      }
    }

    if (helpCommand.isDefined || noArguments) {
      println(
        """
        |Usage: SimpleSparkApp [options]
        |
        |Options:
        |  --init  Initialize the database and the table
        |  --fill  Fill the database with fake data
        |  --help  Print this help message
        |
        |""".stripMargin
      )
      // Exit 0
      if (noArguments)
        System.exit(1)
      else
        System.exit(0)
    }

    // If the --init argument is present, initialize the database and the table
    if (initCommand.isDefined){
      DatabaseHelper.dropTable(jdbcUrl, jdbcUser, jdbcPassword)
      DatabaseHelper.createTable(jdbcUrl, jdbcUser, jdbcPassword)
      // Exit 0
      System.exit(0)
    }


    // If the --fill argument is present, fill the database with fake data
    if (fillCommand.isDefined) {
      // Fill the database with fake data
      DatabaseHelper.fillData(
        jdbcUrl,
        jdbcUser,
        jdbcPassword,
        fillCommand.get.numUsers
      )
      // Exit 0
      System.exit(0)
    }

    val spark = SparkSession
      .builder()
      .appName("A sample Spark app")
      // Set master to local if you want to run it locally
      .master("local")
      .config("spark.some.config.option", "config-value")
      // Configure Delta support for Spark
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    // User(id, firstName, lastName, age, numFriends, timestamp)

    val columns =
      Seq("id", "firstName", "lastName", "age", "numFriends", "date_created")
    val keyColumns = List("id")
    val partitionColumns = List("age")
    val updatesColumn = "date_created"
    val createdColumn = "date_created"

    // Create a list of users
    val users = (1 to 100).map(_ => Faker.randomUser()).toList
    val userSarah =
      User(1, "Smith", "Sarah", 30, 100, Timestamp.from(Instant.now()))

    // Create a DataFrame from the list of users
    val usersDF = spark.createDataFrame(users)

    // Create another DataFrame from the list of users that have the same id, choose the latest date_created if there are duplicates rows
    val userSara = spark.createDataFrame(List(userSarah))

    // Merge the two DataFrames
    val usersDFMerged = usersDF.union(userSara)




    // Create deduplicated DataFrame from the list of users that have the same id, choose the latest date_created if there are duplicates rows
    val usersDFDistinct = usersDF
      .orderBy(col(updatesColumn).desc)
      .dropDuplicates(keyColumns)

    // Show the DataFrame
    //
    // +---+-----+---+----------+
    // | id| name|age|numFriends|
    // +---+-----+---+----------+
    // |  1| John| 33|       100|
    // |  2| Mary| 22|       200|
    // |  3|Peter| 44|       300|

    usersDF.show()

    // Show the deduplicated DataFrame
    usersDFDistinct.show()

    // Calculate the size in bytes of the DataFrame in memory using Catalyst
    val sizeInBytes =
      usersDFDistinct.queryExecution.optimizedPlan.stats.sizeInBytes

    // Calculate the size in bytes of the DataFrame in memory using Spark
    println(f"🚀 Size in bytes of the result: $sizeInBytes")
    // Size in MB
    val sizeInMB: Double = sizeInBytes.toDouble / 1024.0 / 1024.0
    println(f"🚀 Size in MB of the result: $sizeInMB")

    // Calculate the ideal number of partitions, the minimum number of partitions is 1
    val idealNumPartitions = Math.max(1, Math.ceil(sizeInMB / 128).toInt)
    println(f"🚀 Ideal number of partitions: $idealNumPartitions")

    // Repartition the DataFrame
    val usersDFRepartitioned =
      usersDFDistinct.repartition(idealNumPartitions)

    // Write the DataFrame to parquet
    usersDFRepartitioned.write
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy(partitionColumns: _*)
      .mode("overwrite")
      .save("./users.parquet")

    // Read the DataFrame from parquet
    val usersDFRead = spark.read.parquet("./users.parquet")

    // Select the users with name "Sara" using SQL
    println("👉 Select the users with name 'Sara' using SQL")
    usersDFRead.createOrReplaceTempView("users")
    val usersDFReadSara =
      spark.sql("SELECT * FROM users WHERE firstName = 'Sara'")
    usersDFReadSara.show()

    // Save the usersDFRepartitioned DataFrame as a Delta table
    println("👉 Save the usersDFRepartitioned DataFrame as a Delta table")

    // if the table does not exist, create it
    // Test if the directory exists and if it is empty

    val path = "./users.delta"
    val directory = new java.io.File(path)

    if (!directory.exists || directory.listFiles.isEmpty) {

      print("👉 Creating the Delta table")

      usersDFRepartitioned.write
        .format("delta")
        .option("compression", "snappy")
        .partitionBy(partitionColumns: _*)
        .save(path)

    } else {

      println("👉 The table already exists")
      val targetTable = DeltaTable.forPath(spark, "./users.delta")

      val mergeExpr: Unit = targetTable
        .as("target")
        .merge(
          usersDFRepartitioned.as("source"),
          "target.id = source.id  AND target.date_created > source.date_created"
        )
        .whenMatched
        .updateExpr(
          Map(
            "id" -> "source.id",
            "firstName" -> "source.firstName",
            "lastName" -> "source.lastName",
            "age" -> "source.age",
            "numFriends" -> "source.numFriends",
            "date_created" -> "source.date_created"
          )
        )
        .whenNotMatched
        .insertExpr(
          Map(
            "id" -> "source.id",
            "firstName" -> "source.firstName",
            "lastName" -> "source.lastName",
            "age" -> "source.age",
            "numFriends" -> "source.numFriends",
            "date_created" -> "source.date_created"
          )
        )
        .execute()

    }

    // Stop the SparkSession

    spark.stop()

  }
}
