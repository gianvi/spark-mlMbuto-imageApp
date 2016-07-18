package ImageClassificationAndRetrieval.ImageRetrievalJobs.KNNModels

import core.SparkJob
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.{Log => ParquetLog}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.KNNClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.linalg.{DenseVector, Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

/**
  * Created by gian on 30/05/16.
  */
class Base(data: Option[DataFrame], loadPath: Option[String], savePath: Option[String])
  extends SparkJob[Unit]
    with Serializable {


  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {


    val features = data match {
      case None => loadParquet(sqlContext)
      case Some(d) => d
    }

    val clusterColumns = for (c <- features.columns if c != "image" && c != "label" && c!= "stringLabel") yield c
    println("Cluster columns to vectorize: "+clusterColumns.mkString(" "))

    val inputData = features
      .withColumnRenamed("label", "stringLabel")

    inputData.show

//        val summaryStats = inputData.describe()
//        summaryStats.show

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
      .select("image", "stringLabel", "label", "features")
      .randomSplit(Array(0.7, 0.3), seed = 1234L)
      .map(_.cache())



    labeledData.show
    train.show


    val treeSize = assembledData.count.toInt / 3
    println(s"Dimensione dell'albero: ${treeSize} su un dataset (test+train) di ${cc}")
    println("Training set:"+train.count)
    println("Test set:"+test.count)

    val knn = new KNNClassifier()
      .setK(2)
      .setTopTreeSize(2)
      .setTopTreeLeafSize(8)
      .setSubTreeLeafSize(8)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setBalanceThreshold(0)
      .setBufferSizeSampleSizes(Array(1,2,3,4,5))

    println(s"KNN Base model params:\n ${knn.explainParams}")

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
    println("\n Precision...???accuracy")
    println(s"In-sample: $insample, Out-of-sample: $outofsample")
//


  }

  private[this] def validate(results: DataFrame): Double = {
    val precision: Double = results
      .selectExpr("SUM(CASE WHEN label = prediction THEN 1.0 ELSE 0.0 END) / COUNT(1)")
      .collect()
      .head
      .getDecimal(0)
      .doubleValue()

    precision
  }


  private[this] def loadParquet(sqlContext: SQLContext): DataFrame ={
    //    val parquetJLogger = JLogger.getLogger(classOf[ParquetLog].getPackage.getName)
    //    parquetJLogger.setLevel(JLevel.OFF)
    //    println("Class name of JAVA parquet log: "+parquetJLogger.getName)

    val parquetLogger = Logger.getLogger(classOf[ParquetLog].getPackage.getName)
    parquetLogger.setLevel(Level.OFF)
    println("Class name of LOG4J parquet log: "+parquetLogger.getName)

    //    parquetJLogger.getHandlers.foreach(parquetJLogger.removeHandler)
    //    parquetJLogger.getHandlers.foreach(parquetJLogger.removeHandler)
    //    parquetJLogger.setUseParentHandlers(true)



    val training = sqlContext
      .read
      .format("parquet")
      .load(loadPath.get)
      .withColumnRenamed("label", "stringLabel")
      .na.drop

    training


    //TODO foreach features column...
    //    val df2 = training.explode[Double, Boolean]("0", "isNaN")(d => Seq(d.isNaN))
    //    println("filter nan: ")
    //    df2.filter($"isNaN" !== true).show


//
//    val pred = sqlContext
//      .read
//      .format("parquet")
//      .load("/home/gian/Workspace/Actual/spark-ml-mbuto/data/ImageRetrievalApp/7classification/1465767534086/base/predictionTest.parquet")
//      .na.drop
//    println("Test:  "+pred.count)
//    pred.show()
//    pred
  }
}