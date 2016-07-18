name := "spark-ml-mbuto"

version := "1.0"

scalaVersion := "2.11.8"

//sovrascrive le scalaVersion values (warning dependencies)
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


libraryDependencies := Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.5.2",
  "org.apache.spark" % "spark-mllib_2.11" % "1.5.2",
  "org.apache.spark" % "spark-sql_2.11" % "1.5.2",
  "com.databricks" % "spark-csv_2.11" % "1.2.0",


  //per image retrieval pipeline
  "org.openimaj" % "core-image" % "1.3.1",
  "org.openimaj" % "image-feature-extraction" % "1.3.1",
  "org.openimaj" % "image-local-features" % "1.3.1",


  //knn
  "saurfang" % "spark-knn" % "0.1.0"

)
    