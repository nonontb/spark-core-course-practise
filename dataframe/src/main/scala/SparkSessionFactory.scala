import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def getSparkSession: SparkSession =
    SparkSession.builder().master("local").getOrCreate()
}
