import jdbcspark.DatabaseHelper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DatabaseHelperTest
    extends AnyFunSuite
    with PostgresContainer
    with Matchers {

  import jdbcspark.DatabaseHelper

  def nbUsers = 1000

  // Write a beforeAll method that creates a table in the database
  override def beforeAll(): Unit = {
    super.beforeAll()
    DatabaseHelper.createTable(
      this.getUrl(),
      this.getUser(),
      this.getPassword()
    )
  }

  test(s"fill the database with ${nbUsers} users rows") {
    DatabaseHelper.fillData(
      this.getUrl(),
      this.getUser(),
      this.getPassword(),
      nbUsers
    )
    // Check that the database has nbUsers rows in users table
    val connection = this.getConnection()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT COUNT(*) FROM users")
    resultSet.next()
    resultSet.getInt(1) should be(nbUsers)
  }

}
