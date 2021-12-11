import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession


class SparkSessionFactoryTest extends AnyFlatSpec with Matchers {

  "SparkSessionFactory" should "create a local spark session" in {
    SparkSessionFactory.getSparkSession shouldBe a [SparkSession]
    SparkSessionFactory.getSparkSession.conf.get("spark.master") shouldBe "local"
  }
  
}
