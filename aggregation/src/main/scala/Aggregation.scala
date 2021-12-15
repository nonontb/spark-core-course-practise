import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.WindowSpec

object Aggregation {

  // REMEMBER , You alwways need a SparkSession
  val spark:SparkSession = ???
  import spark.implicits._
  //load using json datasource as dataset
  // ./data/aggregations/authors.json
  val dsPerson:Dataset[Author] = ???

  //Convert dataset to RDD
  val rdd = ???

  //Convert dataset to Dataframe 
  val df = ??? 

  def coAuthoredBooksWithRDD(rddAuthor:RDD[Author]):Seq[String] = {
    val resultRDD:RDD[String] = ???
    resultRDD.collect().toSeq
  }

  def coAuthoredBooksWithDS(rddAuthor:Dataset[Author]):Seq[String] = {
    val resultDS:Dataset[String] = ???
    resultDS.collect().toSeq
  }

  def coAuthoredBooksWithDF(rddAuthor:DataFrame):Seq[String] = {
    val resultDF:DataFrame = ???
    resultDF.collect().map(r => r.getAs[String](1)).toSeq
  }

    def coAuthoredBooksWithWindowFunction(rddAuthor:DataFrame):Seq[String] = {
    val windowSpec:WindowSpec = ???  
    val resultDF:DataFrame = ???
    resultDF.collect().map(r => r.getAs[String](1)).toSeq
  }
    
}

case class Author(
  id:Int,
  firstName:String,
  lastName:String,
  title:String,
  editor:String)
