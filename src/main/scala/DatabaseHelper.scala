package jdbcspark

// Import jdbc drivermanager
import java.sql.DriverManager

object DatabaseHelper {

  // drop a table in the database
  def dropTable(
      jdbcUrl: String,
      jdbcUser: String,
      jdbcPassword: String
  ): Unit = {
    // open a JDBC connection to the database
    val connection =
      DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    // create a statement
    val statement = connection.createStatement()
    // execute the statement
    statement.execute("DROP TABLE IF EXISTS users")
    // close the statement
    statement.close()
    // close the connection
    connection.close()
  }

  // create a table in the database
  def createTable(
      jdbcUrl: String,
      jdbcUser: String,
      jdbcPassword: String
  ): Unit = {
    // open a JDBC connection to the database
    val connection =
      DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    // create a statement
    val statement = connection.createStatement()
    // execute the statement
    statement.execute(
      "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, firstName TEXT, lastName TEXT, age INT, numFriends INT, date_created TIMESTAMP)"
    )
    // close the statement
    statement.close()
    // close the connection
    connection.close()
  }

  // fillDatabase with fake user data using a JDBC connection to a database
  def fillData(
      jdbcUrl: String,
      jdbcUser: String,
      jdbcPassword: String,
      numUsers: Int
  ): Unit = {
    // We want to insert the users into the database in bulk of 1000 users
    val batchSize = 1000
    // Calculate the number of batches we need to insert all the users
    // We use the ceil function to round up the number of batches
    val numBatches = math.ceil(numUsers / batchSize).toInt
    // print the number of batches on the console
    println(
      f"🚀 Number of batches: $numBatches to execute $numUsers of user to insert into the database"
    )
    // open a JDBC connection to the database
    val connection =
      DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    // iterate over the batches
    (1 to numBatches).foreach { batch =>
      // print the batch index on the console
      println(f"👉 Batch $batch/$numBatches")
      // generate a list of random users
      val users = (1 to batchSize).map(n => Faker.randomUser(Some(batch*batchSize+n))).toList
      // Insert the users into the database in bulk of 1000 users
      val sql =
        "INSERT INTO users (id, firstName, lastName, age, numFriends, date_created) VALUES (?, ?, ?, ?, ?, ?)"
      val statement = connection.prepareStatement(sql)
      // iterate over the users in the batch
      users.foreach { user =>
        statement.setInt(1, user.id)
        statement.setString(2, user.firstName)
        statement.setString(3, user.lastName)
        statement.setInt(4, user.age)
        statement.setInt(5, user.numFriends)
        statement.setTimestamp(6, user.date_created)
        statement.addBatch()
      }
      statement.executeBatch()
    }
    // close the JDBC connection
    connection.close()
    // print the number of batches on the console
    println(f"👍 Finished inserting $numUsers users into the database")
  }
}
