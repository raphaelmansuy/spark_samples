import org.apache.spark.sql.SparkSession

object ParallelDemo {
	def generateRandomNumberAndSum: Int = {
		val spark = SparkSession.builder
			.master("local[*]")
			.appName("ParallelDemo")
			.getOrCreate()
		val sc = spark.sparkContext
		// Create 100 tasks
		// Generate a random number and sum it up
		val res = sc.parallelize(1 to 100)
			.map(_ => scala.util.Random.nextInt(10))
			.reduce(_ + _)
		res
	}

}