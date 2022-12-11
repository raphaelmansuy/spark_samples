
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class FakerTest extends AnyFunSuite with Matchers {
import jdbcspark.Faker
  test("generate a fake user with a random id") {
    val user = Faker.randomUser()
    user.id should be > 0
    user.id should be < 100000000
    user.firstName should not be empty
    user.lastName should not be empty
    user.age should be > 0
    user.age should be < 100
    user.numFriends should be > 0
    user.numFriends should be < 1000
  }

  test("generate a fake user with a specific id") {
    val user = Faker.randomUser(Some(1))
    user.id should be(1)
    user.firstName should not be empty
    user.lastName should not be empty
    user.age should be > 0
    user.age should be < 100
    user.numFriends should be > 0
    user.numFriends should be < 1000
  }
}
