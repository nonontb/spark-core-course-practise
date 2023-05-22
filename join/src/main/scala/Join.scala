import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Join {

  // REMEMBER , You alwways need a SparkSession
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  //load using json datasource as dataset

  // ./data/join/movies.json
  val dfMovies: DataFrame = spark.read.json("./data/join/movies.json")
  //Convert dataset to DataSet
  val dsMovies: Dataset[Movies] = dfMovies.as[Movies]
  //./data/join/movie_cat.json
  val dfCategory: DataFrame = spark.read.json("./data/join/movie_cat.json")
  //Convert dataset to DataSet
  val dsCategory: Dataset[Category] = dfCategory.as[Category]

  def addCategoryMoviesWithDS(dsMovies: Dataset[Movies], dsCategory: Dataset[Category]): Seq[CompleteMovies] = {
    val resultDS: Dataset[CompleteMovies] = dsMovies.joinWith(dsCategory, dsMovies("category") === dsCategory("id"), "inner")
      .map({ case (movie, cat) => CompleteMovies(movie.id, movie.title, movie.director, cat.category) })
    resultDS.collect().toSeq
  }

  def addCategoryMoviesWithDF(dfMovies: DataFrame, dfCategory: DataFrame): Seq[Row] = {
    val resultDF: DataFrame = dfMovies.join(dfCategory, dfMovies("category") === dfCategory("id"))
      .select(dsMovies("id"), dsMovies("title"), dsMovies("director"), dfCategory("category"))
      .drop(dfCategory("id"))
    resultDF.collect().toSeq
  }

  def filterMoviesWithoutCategoryUsingJoin(dsMovies: Dataset[Movies], dsCategory: Dataset[Category]): Seq[Movies] = {
    val resultDS: Dataset[Movies] = dsMovies.join(dsCategory, dfMovies("category") === dsCategory("id"), "anti")
      .as[Movies]
    resultDS.collect().toSeq
  }
}

