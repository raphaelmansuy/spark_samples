package jdbcspark

import org.apache.spark.sql.DataFrame
import jdbcspark.JDBCDriverEnumeration.JDBCDriver

// This class is used to data from a JDBC source into a Spark DataFrame
class JDBCLoader(url: String, user: String, password: String, driver: JDBCDriver) {
	// get the driver class 
	private val driverClass = driver

  private def defaultJDBCOptions: Map[String, String] = {
    Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "driver" -> driverClass.toString
    )
  }

  private def getJDBCOptions(partitionColumn: String, lowerBound: Int, upperBound: Int, numPartitions: Int): Map[String, String] = {
    Map(
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString
    )
  }

  def load(spark: org.apache.spark.sql.SparkSession,table: String): DataFrame  = {
		val jdbcDF = spark.read
			.format("jdbc")
            .options(defaultJDBCOptions)
            .option("dbtable", table)
			.load()
		jdbcDF
  }

  private def _load(spark: org.apache.spark.sql.SparkSession,table:String, partitionColumn: String, lowerBound: Int, upperBound: Int, numPartitions: Int): DataFrame  = {
    val jdbcDF = spark.read
        .format("jdbc")
        .options(defaultJDBCOptions ++ getJDBCOptions(partitionColumn, lowerBound, upperBound, numPartitions))
         .option("dbtable", table)
        .load()
    jdbcDF
  }

   def load(spark: org.apache.spark.sql.SparkSession,table:String, partitionColumn: String, lowerBound: Int, upperBound: Int, numPartitions: Int, maxConnection: Int = 1): DataFrame  = {

      // generate a sequence of partitions
    val numPartitionsPerConnection =  Math.max(1,numPartitions /  Math.max(1,maxConnection))
    val partitions = (lowerBound to upperBound by (upperBound - lowerBound) / numPartitionsPerConnection).sliding(2).toList
    // use _load to create a list of dataframes
    val dataframes = partitions.map(p => _load(spark, table, partitionColumn, p(0), p(1), maxConnection))
    // union all dataframes
    val unionDF = dataframes.reduce((df1, df2) => df1.union(df2))
    unionDF
  }


  def safeLoad(spark: org.apache.spark.sql.SparkSession, table: String, partitionColumn: String, lowerBound: Int, upperBound: Int, numPartitions: Int): DataFrame = {
    // generate a sequence of partitions
    val partitions = (lowerBound to upperBound by (upperBound - lowerBound) / numPartitions).sliding(2).toList
    // create a list of predicates
    val predicates = partitions.map(p => s"${partitionColumn} >= ${p(0)} AND ${partitionColumn} < ${p(1)}")
    // create a list of dataframes
    val dataframes = predicates.map(p => spark.read
      .format("jdbc")
      .options(defaultJDBCOptions)
      .option("dbtable", table)
      .load())
    // union all dataframes
    val unionDF = dataframes.reduce((df1, df2) => df1.union(df2))
    unionDF
  }


}

	

