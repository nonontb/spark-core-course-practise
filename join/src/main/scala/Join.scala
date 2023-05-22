import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Join {

  // REMEMBER , You alwways need a SparkSession
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  //load using json datasource as dataset
  // ./data/merge/movies.json
  val dfMovies: DataFrame = spark.read.json("./data/join/movies.json")
  //Convert dataset to DataSet
  val dsMovies: Dataset[Movies] = ???
  //./data/merge/movie_cat.json
  val dfCategory: DataFrame = ???
  //Convert dataset to DataSet
  val dsCategory: Dataset[Category] = ???

  def addCategoryMoviesWithDS(dsMovies: Dataset[Movies], dsCategory: Dataset[Category]): Seq[CompleteMovies] = ???

  def addCategoryMoviesWithDF(dfMovies: DataFrame, dfCategory: DataFrame): Seq[Row] = ???

  def filterMoviesWithoutCategoryUsingJoin(dsMovies: Dataset[Movies], dsCategory: Dataset[Category]): Seq[Movies] = ???
}
