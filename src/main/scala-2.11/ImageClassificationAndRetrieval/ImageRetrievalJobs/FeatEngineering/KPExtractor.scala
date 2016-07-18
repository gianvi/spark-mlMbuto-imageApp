package ImageClassificationAndRetrieval.ImageRetrievalJobs.FeatEngineering

import java.io.Serializable

import ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels.{LocalImageData, LocalKPData, LocalTypeData}
import core.SparkJob
import org.apache.parquet.{Log => ParquetLog}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.openimaj.feature.local.list.LocalFeatureList
import org.openimaj.image.FImage
import org.openimaj.image.feature.dense.gradient.dsift._

import scala.collection.JavaConverters._


/**
  * Created by gian on 11/05/16.
  */
class KPExtractor(images: Option[RDD[LocalTypeData]], savePath: Option[String])
  extends SparkJob[RDD[LocalKPData]]
    with Serializable {

  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): RDD[LocalKPData] = {

    //Extract local keypoints for each image
    val keypoints: RDD[LocalKPData] = images match {
      case None => sc.emptyRDD[LocalKPData]
      case Some(imgs) =>
        imgs.map{
          i => {
            val sift: AbstractDenseSIFT[FImage] = getSift

            sift.analyseImage(i.getImage)

            val byteKeypoints: LocalFeatureList[ByteDSIFTKeypoint] = sift.getByteKeypoints(0.005f)
            new LocalKPData(i.getPath, i.getType, byteKeypoints)
          }
        }
    }


    //Write keypoints to file
    savePath match {
      case Some(path) => {
        val kps = keypoints.collect.toList.asJava
        LocalImageData.writeList(path, kps)
        println("Keypoints saved in: "+path)
      }
      case None => ()
    }


    println(s"Keypoints extracted for: ${keypoints.count} images.")
    keypoints.take(5).foreach(im => println(s"L'immagine ${im.getPath.substring(55, im.getPath.length-1)} contiene: " +
      s" ${im.getKeypoints.size} [${im.getKeypoints.vecLength}] keypoints"))

    keypoints

  }


  private[this] def getSift: AbstractDenseSIFT[FImage] = {
    val dsift: DenseSIFT = new DenseSIFT(5, 7)
    new PyramidDenseSIFT[FImage](dsift, 6f, 7)
  }

}
