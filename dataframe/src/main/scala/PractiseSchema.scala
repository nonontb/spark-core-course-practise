import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object PractiseSchema {
  
  // getSparkSession needs to be implemented
  val spark:SparkSession = SparkSessionFactory.getSparkSession

  // path ./data/dataframe/schema/vins.csv
  val csvDF:DataFrame = spark.read.option("header",true).csv("./data/dataframe/schema/vins.csv")

  // path ./data/dataframe/schema/vins.json
  val jsonDF:DataFrame = spark.read.json("./data/dataframe/schema/vins.json")

 
  // schema should have columns: "appelation", "region", "couleur"
  // tip: print the current schema
  def changeSchemaCsvDF(df:DataFrame):DataFrame = {
    df.withColumnRenamed("name", "appelation")
      .withColumnRenamed("color","couleur")
  }

  // schema should have columns: "appelation", "region", "couleur"
  def changeSchemaJsonDF(df:DataFrame):DataFrame = {
    df.withColumnRenamed("vignoble", "region")
      .withColumn("couleur",when(col("type") === 1,"blanc").otherwise(when(col("type") === 2,"rouge")))
      .drop("type")
  }

  // implement union of both dataset now they have the same column
   def mergeDataset(df1:DataFrame,df2:DataFrame) = df1.unionByName(df2)

  def run = {
    mergeDataset(changeSchemaCsvDF(csvDF),changeSchemaJsonDF(jsonDF)).show(truncate = false)
  }
}