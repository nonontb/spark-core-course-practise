import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import javax.xml.crypto.Data

object PractiseColumns {
  // Create a col from a literal
  def createConst(df:DataFrame,colName:String,value:Any):DataFrame = ???

  //replace df value col per a padded one : use lpad
  def replaceWithPaddedCol(df:DataFrame,colName:String, len:Int,paddedChar:String):DataFrame = ???

  // create a col fullName from firstName and lastName: use concat
  def concatColumns(df:DataFrame, newColName:String, srcCols:Seq[Column]):DataFrame = ???

  // convert value (date as str) to date type : use to_date
  def dateStrAsDate(df:DataFrame,dateAsStrCol:String):DataFrame = ???

  //convert tab value field into mulitple lines
  def explodeFieldIntoLine(df:DataFrame,tabColName:String,explodedColName:String):DataFrame = ???

  //convert  value field into list of all fields value: use collect_list 
  def collectField(df:DataFrame,colName:String,collectedColName:String):DataFrame = ???

  //copy id from root into a existing nested struct nested.id: use withField
  def addNestedField(df:DataFrame,colName:String):DataFrame = ???

  //delete existing nested.toReplaceColName by id from root: use dropField
  def deleteNestedField(df:DataFrame,toDeleteColName:String):DataFrame = ???


}
