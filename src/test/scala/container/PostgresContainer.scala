import com.dimafeng.testcontainers.GenericContainer
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.containers.BindMode

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.JavaConverters.mapAsJavaMapConverter

trait PostgresContainer extends BeforeAndAfterAll {
  self: Suite =>
  protected var postgresDb: GenericContainer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    postgresDb = GenericContainer(
      "postgres",
      Seq(5432)
    )

    postgresDb.container.withEnv(
      Map[String, String](
        "POSTGRES_USER" -> getUser(),
        "POSTGRES_PASSWORD" -> getPassword(),
        "POSTGRES_DB" -> getDatabase
      ).asJava
    )

    /*postgresDb.container.addFileSystemBind(
      getClass.getResource("/data").getPath,
      "/docker-entrypoint-initdb.d/",
      BindMode.READ_ONLY
    )*/

    postgresDb.start()

  }

  protected def getConnection(): Connection = {
    val con_st = getUrl()
    val connectionProperties = new Properties()
    connectionProperties.put("user", getUser())
    connectionProperties.put("password", getPassword())
    DriverManager.getConnection(con_st, connectionProperties)
  }

  protected def getUrl(): String = {
    val database = getDatabase()
    s"jdbc:postgresql://localhost:${postgresDb.container.getMappedPort(5432)}/$database"
  }

  protected def getDatabase(): String = {
    "db"
  }

  protected def getUser(): String = {
    "test"
  }

  protected def getPassword(): String = {
    "test"
  }


}
