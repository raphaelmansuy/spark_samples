import org.scalatest.funsuite.AnyFunSuite
import jdbcspark.JDBCDriverEnumeration

class JDBCDriverEnumerationTest extends  AnyFunSuite {

	 test("JDBCDriverEnumerationTest") {
	 	val driver = JDBCDriverEnumeration.MySQL.toString
			// assert 
			assert(driver == "com.mysql.jdbc.Driver")
	 }
  
}
