import org.scalatest.flatspec._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.matchers.should.Matchers

class JoinTest extends AnyFlatSpec with Matchers {

  val expectedCompleteMovies = Seq(
    CompleteMovies(
      id = 1,
      title = "Star wars, A new Hope",
      director = "George Lucas",
      category = "sci-fi"
    ),
    CompleteMovies(
      id = 2,
      title = "Alien",
      director = "Ridley Scott",
      category = "sci-fi"
    ),
    CompleteMovies(
      id = 3,
      title = "Pulp Fiction",
      director = "Quentin Tarantino",
      category = "thriller"
    ),
    CompleteMovies(
      id = 4,
      title = "Seven",
      director = "David Fincher",
      category = "thriller"
    ),
    CompleteMovies(
      id = 5,
      title = "La grande vadrouille",
      director = "Gérard Oury",
      category = "comedy"
    ))

  val toRow: CompleteMovies => Row = (c:CompleteMovies) => Row(c.id,c.title,c.director,c.category)

  "Join" should "enrich Movies with Category using DataSet" in {
    val result = Join.addCategoryMoviesWithDS(Join.dsMovies, Join.dsCategory)

    result should contain theSameElementsAs expectedCompleteMovies
  }

  it should "enrich Movies with Category using using DataFrame" in {
    val result = Join.addCategoryMoviesWithDF(Join.dfMovies,Join.dfCategory)

    val expectedRow: Seq[Row] = expectedCompleteMovies.map(toRow)
    result should contain theSameElementsAs expectedRow
  }

  it should "filter Comedy using Join" in {
    val result = Join.filterMoviesWithoutCategoryUsingJoin(Join.dsMovies, Join.dsCategory)

    val expectedComedy: Seq[Movies] = Seq(Movies(
      id = 6,
      title = "Avatar",
      director = "James Cameron"
    ))
    result should contain theSameElementsAs expectedComedy
  }
}
