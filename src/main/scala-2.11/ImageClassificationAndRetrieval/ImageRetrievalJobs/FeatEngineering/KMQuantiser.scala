package ImageClassificationAndRetrieval.ImageRetrievalJobs.FeatEngineering

import ImageClassificationAndRetrieval.ImageRetrievalJobs.ConfImageRetrieval
import ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels.dscala.KeypointData
import ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels.{LocalCentroidsData, LocalKPData}
import core.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeansModel => KmeansModelMllib, KMeans => KmeansMllib}
import org.apache.spark.ml.clustering.{KMeansModel => KmeansModelML, KMeans => KmeansML}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.openimaj.feature.local.data.LocalFeatureListDataSource
import org.openimaj.feature.local.list.LocalFeatureList
import org.openimaj.image.feature.dense.gradient.dsift.ByteDSIFTKeypoint
import org.openimaj.ml.clustering.ByteCentroidsResult
import org.openimaj.ml.clustering.kmeans.ByteKMeans

import scala.collection.JavaConverters._


/**
  * Created by gian on 11/05/16.
  */


class KMQuantiser (keypoints:RDD[LocalKPData], mode: String, savePath: Option[String], loadPath: Option[String])//, minPartitions: Int = 8)
  extends SparkJob[DataFrame]
    with Serializable {


  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {


    mode match {
      //case "OpenImaj"   => trainOpenImajQuantiser(sc)                 //ByteCentroidsResult
      case "SparkML"    => trainSparkMLQuantiser(sc, sqlContext)        //KmeansModelML, Dataframe
      case "SparkMLLib" => trainSparkMLLibQuantiser(sc, sqlContext)     //KmeansModelMllib, RDD[KeypointData]
    }

  }





  private def trainOpenImajQuantiser(sc: SparkContext): ByteKMeans = {
    val dictionary = keypoints.map(im => im.getKeypoints).collect.toList.asJava
    println(s"Dictionary of keypoints list for each image, size: ${dictionary.size}")

    val datasource: LocalFeatureListDataSource[ByteDSIFTKeypoint, Array[Byte]] = new LocalFeatureListDataSource(dictionary)
    println(s"Datasource size: ${datasource.numRows}")
    println(s"Datasource dims:${datasource.numDimensions}")

    val clusterNum = (1D/100D * datasource.numRows.toDouble)

    println("Universo U dei kps: " + datasource.numRows + " clusters(" + 1 + " / " + 100 + ")*" + datasource.numRows + "=" + clusterNum)
    val km: ByteKMeans = ByteKMeans.createKDTreeEnsemble(clusterNum.toInt)
    val clusters: ByteCentroidsResult = km.cluster(datasource)
    println(s"Clusters num: ${clusters.numClusters}")
    println(s"Clsuter dims: ${clusters.numDimensions}")

    //val clustersRDD: RDD[Array[Byte]] = sc.parallelize(clusters.centroids)
    val lpath = loadPath.getOrElse("")
    if(lpath!=""){
      val data: LocalCentroidsData = new LocalCentroidsData(clusters)
      data.write(lpath+"centroids.ser")
    }

    km
  }




  private def trainSparkMLLibQuantiser(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val featureVectorDictionary: RDD[KeypointData] = keypoints.flatMap{ localFeatList =>
      val javaList: LocalFeatureList[ByteDSIFTKeypoint] = localFeatList.getKeypoints
      val scalaList = javaList.asScala.toList

      scalaList.map(kp => KeypointData(localFeatList.getType, localFeatList.getPath, Vectors.dense(kp.descriptor.map(_.toDouble)), -1))
    }
      .coalesce(4)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val numClusters = (ConfImageRetrieval.KM_PERCENTAGE_CLUSTER * featureVectorDictionary.count.toDouble).toInt
    println("Universo U dei kps: " + featureVectorDictionary.count
      + "\nUniverso C dei clusters: "+numClusters+"  = (" + 1 + " / " + 1000 + ")*" + featureVectorDictionary.count)

    val prediction = loadPath match {

      //build/train model, predict and save model/prediction
      case None => {
        val model: KmeansModelMllib = KmeansMllib.train(
          featureVectorDictionary.map(_.keypoint),
          numClusters,
          ConfImageRetrieval.KM_NUM_ITERATIONS
        )

        val keypointsWithCluster = featureVectorDictionary.map{
          kps => KeypointData(kps.label, kps.image, kps.keypoint, model.predict(kps.keypoint))
        }.toDF()

        savePath match {
          case Some(spath) => {
            model
              .save(sc, spath+ConfImageRetrieval.KM_MODEL_PATH)

            keypointsWithCluster
              .saveAsParquetFile(spath+ConfImageRetrieval.KM_FEAT_PATH)

            println("Clustered keypoints saved in: "+spath+ConfImageRetrieval.KM_FEAT_PATH)
            println("Kmeans model saved in: "+spath+ConfImageRetrieval.KM_MODEL_PATH)
          }
          case None => ()
        }

        println(s"Clusters num: ${model.k}")
        println(s"Clsuter dims: ${model.clusterCenters(0).size}")
        println(s"Clustered keypoints: ${keypointsWithCluster.count}")
        keypointsWithCluster.show
        keypointsWithCluster
      }

      //load model, predict and save prediction
      case Some(lpath) => {

        val model = KmeansModelMllib
          .load(sc, lpath)

        val keypointsWithCluster = featureVectorDictionary.map{
          kps => KeypointData(kps.label, kps.image, kps.keypoint, model.predict(kps.keypoint))
        }.toDF()

        savePath match {
          case Some(spath) => {
            keypointsWithCluster
              .saveAsParquetFile(spath+ConfImageRetrieval.KM_FEAT_PATH)
            println("Clustered keypoints saved in: "+spath+ConfImageRetrieval.KM_FEAT_PATH)

          }
          case None => ()
        }

        println(s"Clusters num: ${model.k}")
        println(s"Clsuter dims: ${model.clusterCenters(0).size}")
        println(s"KeypointsWithCluster: ${keypointsWithCluster.count}")
        keypointsWithCluster.show
        keypointsWithCluster

      }
    }

    prediction
  }





  private def trainSparkMLQuantiser(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    //TODO check dall' 1.6 per save/load modelML
    import sqlContext.implicits._

    //RDD[(String, String, Vector)]
    val featureVectorDictionary = keypoints.flatMap{ localFeatList =>
      val javaList: LocalFeatureList[ByteDSIFTKeypoint] = localFeatList.getKeypoints
      val scalaList = javaList.asScala.toList
      scalaList.map(kp => (localFeatList.getType, localFeatList.getPath, Vectors.dense(kp.descriptor.map(_.toDouble))))//getFeatureVector.getVector)
    }
      .toDF("label", "image", "keypoint")
      .cache

    val numClusters = (ConfImageRetrieval.KM_PERCENTAGE_CLUSTER * featureVectorDictionary.count.toDouble).toInt
    println("Universo U dei kps: " + featureVectorDictionary.count
      + "\nUniverso C dei clusters: "+numClusters+"  = (" + 1 + " / " + 1000 + ")*" + featureVectorDictionary.count)

    val prediction = loadPath match {

      //build/train model, predict and save model/prediction
      case None => {
        val kmeans = new KmeansML()
          .setK(numClusters)
          .setMaxIter(ConfImageRetrieval.KM_NUM_ITERATIONS)
          .setFeaturesCol("keypoint")
          .setPredictionCol("cluster")

        val model: KmeansModelML = kmeans
          .fit(featureVectorDictionary)

        val keypointsWithCluster = model
          .transform(featureVectorDictionary)

        savePath match {
          case Some(spath) => {
            //model
              //.save(sc, spath+"model/")

            keypointsWithCluster
              .saveAsParquetFile(spath+ConfImageRetrieval.KM_FEAT_PATH)

            println("Clustered keypoints saved in: "+spath+ConfImageRetrieval.KM_FEAT_PATH)
            println("You cannot save an ML spark model before 1.6 spark version!")
          }
          case None => ()
        }

        println(s"ML params model: ${model.explainParams}")
        println(s"Clustered Keypoints: ${keypointsWithCluster.count}")
        keypointsWithCluster.show
        keypointsWithCluster
      }

      //load model, predict and save prediction
      case Some(lpath) => {
//        val model = KmeansModelML
//          .load(lpath)
//
//        val prediction: DataFrame = model
//          .transform(featureVectorDictionary)
//
//        prediction
//          .write
//          .format("parquet")
//          .mode(SaveMode.Overwrite)
//          .partitionBy("image")
//          .save(s"${ConfImageClassificator.SUBPATH_TO_SAVE}/myQuantiserData/predictionML/features/${System.currentTimeMillis()}/kpsAndCluster.parquet")


        println("You cannot load an ML spark model before 1.6 version!")
        sqlContext.emptyDataFrame
      }
    }

    prediction
  }


}
