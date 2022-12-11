package jdbcspark

import scala.Enumeration

object JDBCDriverEnumeration extends Enumeration {
  type JDBCDriver = String 

  val MySQL = Value("com.mysql.jdbc.Driver")
  val PostgreSQL = Value("org.postgresql.Driver")
  val SQLite = Value("org.sqlite.JDBC")
  val H2 = Value("org.h2.Driver")
  val MicrosoftSQL = Value("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  val Oracle = Value("oracle.jdbc.driver.OracleDriver")
  val Sybase = Value("com.sybase.jdbc4.jdbc.SybDriver")
}
