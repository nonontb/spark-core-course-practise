import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object PractiseSchema {
  
  // getSparkSession needs to be implemented
  val spark:SparkSession = SparkSessionFactory.getSparkSession

  // path ./data/dataframe/schema/vins.csv
  val csvDF:DataFrame = ???

  // path ./data/dataframe/schema/vins.json
  val jsonDF:DataFrame = ???

 
  // schema should have columns: "appelation", "region", "couleur"
  // tip: print the current schema
  def changeSchemaCsvDF(df:DataFrame):DataFrame = ???

  // schema should have columns: "appelation", "region", "couleur"
  def changeSchemaJsonDF(df:DataFrame):DataFrame = ???

  // implement union of both dataset now they have the same column
   def mergeDataset(df1:DataFrame,df2:DataFrame):DataFrame = ???

  def run = {
    mergeDataset(changeSchemaCsvDF(csvDF),changeSchemaJsonDF(jsonDF)).show(truncate = false)
  }
}