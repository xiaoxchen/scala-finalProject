name := "Baseball-Prediction-Scala-Spark"

version := "1.0"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  // testing
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // spark core
  "org.apache.spark" % "spark-core_2.10" % "1.5.1",
  "org.apache.spark" % "spark-graphx_2.10" % "1.5.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0",
  // spark packages
  "com.databricks" % "spark-csv_2.10" % "1.2.0"
)