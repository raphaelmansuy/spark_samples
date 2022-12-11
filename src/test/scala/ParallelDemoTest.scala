import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParallelDemoTest extends AnyFunSuite with Matchers {

  test(
    "generateRandomNumberAndSum should generate random numbers and sum them up"
  ) {

    val sum = ParallelDemo.generateRandomNumberAndSum
    sum should be > 0
    // sum should be < 0 and this is the error
    sum should be < 1000000000
    // sum should be < 1000000000
  }

}
