import org.scalatest.funsuite.AnyFunSuite
import jdbcspark.JDBCLoader
import jdbcspark.DatabaseHelper
import org.apache.spark.sql.SparkSession

class JDBCLoaderTest extends AnyFunSuite with PostgresContainer {

  private def nbUsers = 10000

  override def beforeAll(): Unit = {
    super.beforeAll()
    DatabaseHelper.createTable(
      this.getUrl(),
      this.getUser(),
      this.getPassword()
    )
    DatabaseHelper.fillData(this.getUrl(), this.getUser(), this.getPassword(), nbUsers)

  }




  test("JDBCLoaderTest load users table") {
    val loader = new JDBCLoader(this.getUrl(), this.getUser(), this.getPassword(), driver = jdbcspark.JDBCDriverEnumeration.PostgreSQL.toString)
    val spark = SparkSession.builder().master("local").getOrCreate()
    val users = loader.load(spark, "users")

    // display first 100 rows
    users.show(100, truncate = false)
    val nbUsers: Long = users.count()
    println(s"🎅 nbUsers = $nbUsers")
    assert(users.count() == nbUsers)
  }

  test("JDBCLoaderTest load users table with partition") {
    val loader = new JDBCLoader(this.getUrl(), this.getUser(), this.getPassword(), driver = jdbcspark.JDBCDriverEnumeration.PostgreSQL.toString)
    val spark = SparkSession.builder().master("local").getOrCreate()

    val (minId: Int, maxId: Int, count: Int) = getMinMaxCount

    val numPartitions = Math.min(Math.ceil(count.toDouble * 512.0 / (128 * 1024/* *1024.0*/)).toInt, 200)

    println(s"✅ numPartitions = $numPartitions")

    val users = loader.load(spark, "users", "id", minId, maxId, numPartitions)

    // display first 100 rows
    users.show(100, truncate = false)
    val nbUsers = users.count()
    println(s"👩‍ nbUsers = $nbUsers")
    assert(users.count() == nbUsers)
  }

  test("JDBCLoaderTest safeLoad users table with partition") {
    val loader = new JDBCLoader(this.getUrl(), this.getUser(), this.getPassword(), driver = jdbcspark.JDBCDriverEnumeration.PostgreSQL.toString)
    val spark = SparkSession.builder().master("local").getOrCreate()

    val (minId: Int, maxId: Int, count: Int) = getMinMaxCount

    val numPartitions = Math.min(Math.ceil(count.toDouble * 512.0 / (128 * 1024 /* *1024.0*/)).toInt, 200)

    println(s"✅ numPartitions = $numPartitions")

    val users = loader.safeLoad(spark, "users", "id", minId, maxId, numPartitions)

    // display first 100 rows
    users.show(100, false)
    val nbUsers = users.count()
    println(s"👩‍🤖 nbUsers = ${nbUsers}")
    assert(users.count() == nbUsers)
  }


  test("JDBCLoaderTest safeLoad users table with partition and 10 connection") {
    val loader = new JDBCLoader(this.getUrl(), this.getUser(), this.getPassword(), driver = jdbcspark.JDBCDriverEnumeration.PostgreSQL.toString)
    val spark = SparkSession.builder().master("local").getOrCreate()

    val (minId: Int, maxId: Int, count: Int) = getMinMaxCount

    val numPartitions = Math.min(Math.ceil(count.toDouble * 512.0 / (128 * 1024 /* *1024.0*/)).toInt, 200)
    val nbConnections = 10

    println(s"✅ numPartitions = $numPartitions")
    println(s"🔥 nbConnections = $nbConnections")

    val users = loader.load(spark, "users", "id", minId, maxId, numPartitions,nbConnections)

    // display first 100 rows
    users.show(100, false)
    val nbUsers = users.count()
    println(s"👩‍🤖 nbUsers = ${nbUsers}")
    assert(users.count() == nbUsers)
  }

  private def getMinMaxCount = {
    // get min and max id
    val connection = this.getConnection()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT MIN(id), MAX(id), count(*) FROM users")
    resultSet.next()
    val minId = resultSet.getInt(1)
    val maxId = resultSet.getInt(2)
    val count = resultSet.getInt(3)
    println(s"✅ minId = ${minId}, maxId = ${maxId}")
    println(s"✅ nbUsers = $count")
    (minId, maxId, count)
  }
}

