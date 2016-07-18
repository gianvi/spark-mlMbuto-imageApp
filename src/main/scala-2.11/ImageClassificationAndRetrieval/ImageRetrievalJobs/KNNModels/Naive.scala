package ImageClassificationAndRetrieval.ImageRetrievalJobs.KNNModels

import core.{LogUtility, SparkJob}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.NaiveKNNClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.util.logging.{Level => JLevel}

import org.apache.log4j.{Level => L4JLevel}
import org.apache.parquet.{Log => ParquetLog}

/**
  * Created by gian on 30/05/16.
  */
class Naive(data: Option[DataFrame], loadPath: Option[String], savePath: Option[String])
  extends SparkJob[Unit]
    with Serializable {


  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {
    LogUtility.setLogger(sc, "INFO", JLevel.INFO, L4JLevel.INFO)


    val features = data match {
      case None => loadParquet(sqlContext)
      case Some(d) => d
    }

    val clusterColumns = for (c <- features.columns if c != "image" && c != "label") yield c
    println(clusterColumns.mkString(" "))

    val inputData = features
      .withColumnRenamed("label", "stringLabel")

    inputData.show

    val labelIndexer = new StringIndexer()
      .setInputCol("stringLabel")
      .setOutputCol("label")

    val labeledData = labelIndexer
      .fit(inputData)
      .transform(inputData)

    val featsAssembler = new VectorAssembler()
      .setInputCols(clusterColumns)
      .setOutputCol("features")

    val assembledData = featsAssembler
      .transform(labeledData)
    val cc = assembledData.count

    //split traning and testing
    val Array(train, test) = assembledData
      .randomSplit(Array(0.7, 0.3), seed = 1234L)
      .map(_.cache())

    labeledData.show
    assembledData.show

    val treeSize = assembledData.count.toInt / 3
    println(s"Dimensione dell'albero: ${treeSize} su un dataset (test+train) di ${cc}")
    println("Training set:"+train.count)
    println("Test set:"+test.count)

    val knn = new NaiveKNNClassifier()

    println(s"KNN NAIVE model params:\n ${knn.explainParams}")

    val pipeline = knn.fit(train)

    val predictionTrain = pipeline.transform(train)
    val predictionTest = pipeline.transform(test)

    predictionTrain.show
    predictionTest.show

    savePath match {
      case Some(spath) => {
        predictionTest
          .saveAsParquetFile(spath+"predictionTest.parquet")

        predictionTrain
          .saveAsParquetFile(spath+"predictionTrain.parquet")

        println(s"Test prediction saved in ${spath+"predictionTest.parquet"}")
        println(s"Train prediction saved in ${spath+"predictionTrain.parquet"}")
      }
      case None => ()
    }

    val insample = validate(predictionTrain)
    val outofsample = validate(predictionTest)

    //reference accuracy: in-sample 95% out-of-sample 94%
    println(s"In-sample: $insample, Out-of-sample: $outofsample")

  }



  private[this] def validate(results: DataFrame): Double = {
    results
      .selectExpr("SUM(CASE WHEN label = prediction THEN 1.0 ELSE 0.0 END) / COUNT(1)")
      .collect()
      .head
      .getDecimal(0)
      .doubleValue()
  }

  private[this] def loadParquet(sqlContext: SQLContext): DataFrame ={

    sqlContext
      .read
      .format("parquet")
      .load(loadPath.get)
      .withColumnRenamed("label", "stringLabel")
  }
}