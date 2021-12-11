import org.scalatest._
import matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

class PractiseSchemaTest extends AnyWordSpec with Matchers {

  "IngestionTest" should {

    "Load csv dataframe" in {
      val dfContent = PractiseSchema.csvDF.collect().toSeq
      dfContent should contain theSameElementsAs Seq(
        Row("Sancerre", "Loire", "blanc"),
        Row("Mercurey", "Bourgogne", "rouge"),
        Row("Saint émilion", "Bordeaux", "rouge")
      )
    }

    "Load json dataframe" in {
      val dfContent = PractiseSchema.jsonDF.collect().toSeq
      dfContent should contain theSameElementsAs Seq(
        Row("Montbazillac", 1, "Bordeaux"),
        Row("Morgon", 2, "Beaujolais"),
        Row("Condrieu", 1, "Côte du Rhône")
      )
    }

    "Transform csv file schema" in {
      PractiseSchema.changeSchemaCsvDF(PractiseSchema.csvDF).schema shouldBe StructType(
        Seq(
          StructField("appelation", StringType),
          StructField("region", StringType),
          StructField("couleur", StringType)
        )
      )
    }

    "Transform json file schema" in {
      PractiseSchema.changeSchemaJsonDF(PractiseSchema.jsonDF).schema shouldBe StructType(
        Seq(
          StructField("appelation", StringType),
          StructField("region", StringType),
          StructField("couleur", StringType)
        )
      )
    }


    "Merged dataset csv & json " in {
      val finalCsv = PractiseSchema.changeSchemaCsvDF(PractiseSchema.csvDF)
      val finalJsonCsv =  PractiseSchema.changeSchemaJsonDF(PractiseSchema.jsonDF)
      val unionDF = PractiseSchema.mergeDataset(finalCsv,finalJsonCsv)

      unionDF.schema shouldBe StructType(
        Seq(
          StructField("appelation", StringType),
          StructField("region", StringType),
          StructField("couleur", StringType)
        )
      )

      unionDF.collect().toSeq should contain theSameElementsAs Seq(
        Row("Sancerre", "Loire", "blanc"),
        Row("Mercurey", "Bourgogne", "rouge"),
        Row("Saint émilion", "Bordeaux", "rouge"),
        Row("Montbazillac", "Bordeaux","blanc"),
        Row("Morgon", "Beaujolais","rouge"),
        Row("Condrieu", "Côte du Rhône","blanc")
      )
    }

    "run without error" in {
      PractiseSchema.run
    }
  }
}
