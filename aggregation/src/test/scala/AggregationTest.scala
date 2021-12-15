import org.scalatest.flatspec._
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers



class AggregationTest extends AnyFlatSpec with Matchers {

  "Aggregation" should "compute coAuthoredBooks using RDD" in {
    val result = Aggregation.coAuthoredBooksWithRDD(Aggregation.rdd)

    val expected = Seq("High Performance Spark","Spark, The Definitive guide","Spark in Action, 2nd edition")

    result should contain theSameElementsAs expected
  }

  it should "compute coAuthoredBooks using DS" in {
    val result = Aggregation.coAuthoredBooksWithDS(Aggregation.ds)

    val expected = Seq("High Performance Spark","Spark, The Definitive guide","Spark in Action, 2nd edition")

    result should contain theSameElementsAs expected
  }

  it should "compute coAuthoredBooks using Dataframe" in {
    val result = Aggregation.coAuthoredBooksWithDF(Aggregation.df)

    val expected = Seq("High Performance Spark","Spark, The Definitive guide","Spark in Action, 2nd edition")
    result should contain theSameElementsAs expected
  }

    it should "compute coAuthoredBooks using Dataframe with Window function" in {
    val result = Aggregation.coAuthoredBooksWithWindowFunction(Aggregation.df)

    val expected = Seq("High Performance Spark","Spark, The Definitive guide","Spark in Action, 2nd edition")

    result should contain theSameElementsAs expected
  }

}
