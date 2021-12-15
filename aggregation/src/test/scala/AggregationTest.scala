import org.scalatest.flatspec._
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers


class AggregationTest extends AnyFlatSpec with Matchers {
  
  private val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  val rddTest = spark.sparkContext.parallelize(
      Seq(
        Author(1,"first1","name1","Book1","edition1"),
        Author(2,"first2","name2","Book1","edition1"),
        Author(3,"first3","name3","Book2","edition2"),
        Author(4,"first4","name4","Book2","edition2"),
        Author(5,"first5","name5","Book3","edition3"),
      )
    )


  "Aggregation" should "compute coAuthoredBooks using RDD" in {
    val result = Aggregation.coAuthoredBooksWithRDD(rddTest)

    val expected = Seq("Book1","Book2")

    result should contain theSameElementsAs expected
  }

  it should "compute coAuthoredBooks using DS" in {
    val result = Aggregation.coAuthoredBooksWithDS(spark.createDataset(rddTest))

    val expected = Seq("Book1","Book2")

    result should contain theSameElementsAs expected
  }

  it should "compute coAuthoredBooks using Dataframe" in {
    val result = Aggregation.coAuthoredBooksWithDF(spark.createDataset(rddTest).toDF())

    val expected = Seq("Book1","Book2")

    result should contain theSameElementsAs expected
  }

  it should "compute coAuthoredBooks using Dataframe with Window functions" in {
    val result = Aggregation.coAuthoredBooksWithWindowFunction(spark.createDataset(rddTest).toDF())

    val expected = Seq("Book1","Book2")

    result should contain theSameElementsAs expected
  }
}
