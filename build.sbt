version := "1.0.0"


val sparkVersion = "3.2.0"
val scalaTestVersion = "3.2.10"

val commonSettings = Seq (
  scalaVersion := "2.12.15",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.scalactic" %% "scalactic" % scalaTestVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  ),

  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  Test / logBuffered := false
)

lazy val course = (project in file("."))
  .aggregate(dataframe)
  

lazy val dataframe = (project in file("dataframe"))
  .settings(commonSettings)