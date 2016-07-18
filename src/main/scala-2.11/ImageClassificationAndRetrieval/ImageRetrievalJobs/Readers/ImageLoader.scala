package ImageClassificationAndRetrieval.ImageRetrievalJobs.Readers

import java.io.File

import ImageClassificationAndRetrieval.ImageRetrievalJobs.ConfImageRetrieval
import ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels.{LocalImageData, LocalTypeData}
import core.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._

/**
  * Created by gian on 08/05/16.
  */
class ImageLoader(preloadPath: Option[String], loadPath: String, savePath:Option[String])//, minPartitions: Int = 8)
  extends SparkJob[RDD[LocalTypeData]]
    with Serializable {

  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): RDD[LocalTypeData] = {

    //se non persisti => just read pre-loaded files
    val files = preloadPath match {
      case Some(prep) => reader()
      case None       => loader(loadPath)
    }

    val images = sc.parallelize(files)

    println(s"Images loaded: ${images.count} \n" +
      s"data in ${images.partitions.size} partitions \n" +
      s"with ${images.partitioner.getOrElse("No partitioner")} ")

    images.take(5).foreach(im =>
      println(s"${im.getPath.substring(55, im.getPath.length-1)} - ${im.getImage.getDoublePixelVector.length}pixels - ${im.getType}"))

    images
  }


  /**
    * First stage of the image recognition process. Writes each image path and its
    * corresponding classification type to a file.
    * <p>
    * Input:
    * - The root directory of the training data
    * Ouput:
    * - file containing path/type pairs for each image:
    * img1Path car
    * img2Path car
    * img3Path bicycle
    * img4Path bicycle
    * img5Path motorbike
    * img6Path motorbike
    * ....
    */
  @throws(classOf[Exception])
  private[this] def loader(path:String): List[LocalTypeData] = {

    val d = new File(loadPath)
    if (d.exists && d.isDirectory) {

      val classes = d.listFiles.filter(_.isDirectory).toList
      //println(s"Nel dataset ${d.getPath} ci sono  ${classes.size} gruppi d'immagini (label):")
      //classes.foreach(x => println(" - "+x.getPath))


      val trainingData: List[LocalTypeData] = classes.flatMap {
        x =>
          require(x.isDirectory)

          x
            .listFiles()
            .toList
            .slice(0, ConfImageRetrieval.NUM_TRAINING_IMAGES_PER_CLASS)
            .map(f => new LocalTypeData(f.getAbsolutePath, x.getName))
      }

      if(savePath.isDefined){
        LocalImageData.writeList(savePath.get, trainingData.asJava)
        println("Image data saved in: "+savePath.get)
      }


      trainingData
    }
    else {
      List.empty[LocalTypeData]
    }
  }

  private[this] def reader(): List[LocalTypeData] = {
    //Read preloaded training data
    LocalTypeData.readList(loadPath)
      .asScala
      .toList
      .asInstanceOf[List[LocalTypeData]]

  }
}