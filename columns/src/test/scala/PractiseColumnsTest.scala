import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import javax.xml.crypto.Data
import org.apache.spark.sql.types.DateType
import java.time.LocalDate
import java.sql.Date
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StringType


class PractiseColumnsTest extends AnyFlatSpec with Matchers  {

  private val sparkSession = SparkSession.builder().master("local").getOrCreate()
  import sparkSession.implicits._

  "PractiseColumns" should "create a col from a literal" in {
    val inputDF = sparkSession.createDataset(Seq(1)).toDF()
    
    val resultDF = PractiseColumns.createConst(inputDF,"col",42)

    resultDF.show()

    resultDF.schema.fields.map(_.name).toSeq should contain ("col")
    resultDF.collect().toSeq should contain theSameElementsAs Seq(Row(1,42))
  }

  it should "left pad a field" in {
    val inputDF = sparkSession.createDataset(Seq(42)).toDF()

    val resultDF = PractiseColumns.replaceWithPaddedCol(inputDF,"value",10,"0")

    resultDF.show()

    resultDF.schema.fields.map(_.name).toSeq should contain ("value")
    resultDF.collect().toSeq should contain theSameElementsAs Seq(Row("0000000042"))  
  }

  it should "concat two columns into one" in {
    import DataModel._
    val inputDF = sparkSession.createDataset(Seq(Person("John","Doe"))).toDF()

    val resultDF = PractiseColumns.concatColumns(inputDF,"fullName",Seq(col("firstName"),lit(" "),col("lastName")))

    resultDF.show()

    resultDF.schema.fields.map(_.name).toSeq should contain ("fullName")
    resultDF.collect().toSeq should contain theSameElementsAs Seq(Row("John","Doe","John Doe"))
   
  }

  it should "convert date string into data column" in {
    val inputDF = sparkSession.createDataset(Seq("01-03-2021")).toDF()

    val resultDF = PractiseColumns.dateStrAsDate(inputDF, "value")

    resultDF.schema.fields.toSeq should contain (StructField("value",DateType))

    resultDF.show()

    resultDF.collect().toSeq should contain theSameElementsAs Seq(Row(Date.valueOf(LocalDate.of(2021,3,1))))
  }

  it should "convert a a tab field into multiple line" in {
    val inputDF = sparkSession.createDataset(Seq(Seq(1,2,3))).toDF()

    val resultDF = PractiseColumns.explodeFieldIntoLine(inputDF,"value","exploded_value")

    resultDF.show()

    resultDF.schema.fields.toSeq should contain (StructField("exploded_value",IntegerType, nullable = false))
    resultDF.collect().toSeq should contain theSameElementsAs Seq(
      Row(Seq(1,2,3),1),
      Row(Seq(1,2,3),2),
      Row(Seq(1,2,3),3)
    )
  }

  it should "convert a multiple line fields into one line with tab field" in {
    val inputDF = sparkSession.createDataset(Seq(1,2,3)).toDF()

    val resultDF = PractiseColumns.collectField(inputDF,"value","collected_value")

    resultDF.show()

    resultDF.schema.fields.toSeq should contain (StructField("collected_value",DataTypes.createArrayType(IntegerType,false),nullable = false))
    resultDF.collect().toSeq should contain theSameElementsAs Seq(
      Row(Seq(1,2,3))
    )
  }

  it should "create a nested field" in {
    import DataModel._
    val inputDF = sparkSession.createDataset(Seq(Dummy(1,NestedStruct("field")))).toDF()

    val resultDF = PractiseColumns.addNestedField(inputDF,"id")

    resultDF.show()

    resultDF.schema.fields.toSeq should contain (StructField("nested",StructType(Seq(StructField("value",StringType),StructField("id",IntegerType,false)))))
    resultDF.collect().toSeq should contain theSameElementsAs Seq(
      Row(1,Row("field",1))
    )
  }


  it should "delete somes nested fields" in {
    import DataModel._
    val inputDF = sparkSession.createDataset(Seq(Dummy2(1,NestedStruct2("keep","delete")))).toDF()

    val resultDF = PractiseColumns.deleteNestedField(inputDF,"toDelete")

    resultDF.show()

    resultDF.schema.fields.toSeq should contain (StructField("nested",StructType(Seq(StructField("toKeep",StringType)))))
    resultDF.collect().toSeq should contain theSameElementsAs Seq(
      Row(1,Row("keep"))
    )
  }
}


object DataModel {
   case class Person(firstName:String,lastName:String)

   case class Dummy(id:Int,nested:NestedStruct)
   case class NestedStruct(value:String) 

   case class Dummy2(id:Int,nested:NestedStruct2)
   case class NestedStruct2(toKeep:String, toDelete:String)
}