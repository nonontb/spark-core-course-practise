import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import javax.xml.crypto.Data

object PractiseColumns {
  // Create a col from a literal
def createConst(df:DataFrame,colName:String,value:Any):DataFrame = df.withColumn(colName,lit(value))

//replace df value col per a padded one : use lpad
def replaceWithPaddedCol(df:DataFrame,colName:String, len:Int,paddedChar:String):DataFrame = df.withColumn(colName,lpad(col(colName),len,"0"))

// create a col fullName from firstName and lastName: use concat
def concatColumns(df:DataFrame, newColName:String, srcCols:Seq[Column]):DataFrame = df.withColumn(newColName,concat(srcCols:_*))

// convert value (date as str) to date type : use to_date
def dateStrAsDate(df:DataFrame,dateAsStrCol:String):DataFrame = df.withColumn(dateAsStrCol,to_date(col(dateAsStrCol),"dd-MM-yyyy"))

//convert tab value field into mulitple lines
def explodeFieldIntoLine(df:DataFrame,tabColName:String,explodedColName:String):DataFrame = df.withColumn(explodedColName,explode(col(tabColName)))

//convert  value field into list of all fields value: use collect_list 
def collectField(df:DataFrame,colName:String,collectedColName:String):DataFrame = df.select(collect_list(colName).as(collectedColName))

//copy id from root into a existing nested struct nested.id
def addNestedField(df:DataFrame,colName:String):DataFrame = df.withColumn("nested",col("nested").withField(colName,col(colName)))

//delete existing nested.toReplaceColName by id from root
def deleteNestedField(df:DataFrame,toDeleteColName:String):DataFrame = df.withColumn("nested",col("nested").dropFields(toDeleteColName))


}
