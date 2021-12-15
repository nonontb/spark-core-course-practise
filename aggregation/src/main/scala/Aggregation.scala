import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec

object Aggregation {

  // REMEMBER , You alwways need a SparkSession
  val spark:SparkSession = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  //load using json datasource as dataset
  // ./data/aggregations/authors.json
  val df:DataFrame = spark.read.json("./data/aggregation/authors.json")

  //Convert dataset to Dataframe
  val ds = df.as[Author]


  //Convert dataset to RDD
  val rdd = ds.rdd


  def coAuthoredBooksWithRDD(rddAuthor:RDD[Author]):Seq[String] = {
    // Use groupBy or reduceByKey
    val resultRDD = rddAuthor.map(author => (author.title,1)).reduceByKey((x,y) => x+y ).filter({ case (_,nbAuthors) => nbAuthors > 1}).map({case (title,_) => title})
    resultRDD.collect().toSeq
  }

  def coAuthoredBooksWithDS(dsAuthor:Dataset[Author]):Seq[String] = {

    val resultDS:Dataset[String] = dsAuthor.groupByKey(_.title).count().filter(t => t._2 > 1).map(t => t._1)
    //val resultDS:Dataset[String] = dsAuthor.groupByKey(_.title).mapGroups({case (t,iterAuthors) => (t,iterAuthors.size)}).filter(t => t._2 > 1).map(t => t._1)
    resultDS.collect().toSeq
  }

  def coAuthoredBooksWithDF(dfAuthor:DataFrame):Seq[String] = {
    val resultDF:DataFrame = dfAuthor.groupBy($"title").count().filter($"count" > 1).select($"title")
    resultDF.collect().map(r => r.getAs[String](0)).toSeq
  }

  def coAuthoredBooksWithWindowFunction(dfAuthor:DataFrame):Seq[String] = {
    val windowSpec:WindowSpec = Window.partitionBy("title").orderBy("title")
    val resultDF:DataFrame = dfAuthor
      .withColumn("count",count(concat($"firstname",lit("-"),$"lastname")).over(windowSpec))
      .withColumn("numline",row_number().over(windowSpec))
      .filter($"count" > 1 && $"numline" === 1)
      .select($"title")
    resultDF.collect().map(r => r.getAs[String](0)).toSeq
  }
    
}

case class Author(
  id:Long,
  firstName:String,
  lastName:String,
  title:String,
  editor:String)
