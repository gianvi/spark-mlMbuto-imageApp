package ImageClassificationAndRetrieval.ImageRetrievalJobs.FeatEngineering

import ImageClassificationAndRetrieval.ImageRetrievalJobs.ConfImageRetrieval
import core.SparkJob
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.{Log => ParquetLog}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by gian on 17/05/16.
  */
class CfIifTransformer(data: Option[DataFrame], loadPath: Option[String], savePath: Option[String])//, minPartitions: Int = 8)
  extends SparkJob[DataFrame]
    with Serializable {


  import org.apache.spark.sql.functions._


  private[this] val frequencier: ((Long, Long) => Double) = (kpsCounter: Long, clusterCardinality:Long) => {
    kpsCounter.toDouble / clusterCardinality.toDouble
  }
  private[this] val clusterFrequencyFunc = udf(frequencier)

  private[this] val inverser: ((Int, Int) => Double) = (corpus: Int, distinctImages: Int) => {
    Math.log(corpus.toDouble / distinctImages.toDouble) +3
  }
  private[this] val inverseImageFrequencyFunc = udf(inverser)

  private[this] val mult: ((Double, Double) => Double) = (cf: Double, iif: Double) => {
    cf*iif
  }
  private[this] val cfIifFunc = udf(mult)



  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    Logger.getLogger(classOf[ParquetLog].getPackage.getName).setLevel(Level.OFF)

    val kps = data match {
      case None => {
        sqlContext
          .read
          .load(loadPath.get)
      }
      case Some(d) => d
    }

    println(s"Clustered kps: ${kps.count}")
    kps.show


    val imageClusterFrequency = kps
      .groupBy("cluster", "image", "label")
      .count
      .withColumnRenamed("count", "kpsCount")
      .withColumnRenamed("cluster", "clusterA")

    println(s"ImageClusterFrequency (AKA suddivisione dei kps di un imm. nei cluster): ${imageClusterFrequency.count}")
    imageClusterFrequency.show


    val clusterCardinality = kps
      .groupBy('cluster)
      .agg('cluster, countDistinct('image) as ("imgsInCluster"), count("keypoint") as ("kpsInCluster"))
      .select("cluster" , "imgsInCluster", "kpsInCluster")
      .withColumnRenamed("cluster", "clusterB")
      .withColumn("iif", inverseImageFrequencyFunc(lit(ConfImageRetrieval.CORPUS), col("imgsInCluster")))

    val join = clusterCardinality
      .join(imageClusterFrequency, clusterCardinality("clusterB")===imageClusterFrequency("clusterA"))
      .withColumn("cf", clusterFrequencyFunc(col("kpsCount"), col("kpsInCluster")))
      .withColumn("cfIif", cfIifFunc(col("cf"), col("iif")))
      .withColumnRenamed("clusterA", "cluster")
      .select("cluster", "image", "label", "imgsInCluster", "kpsInCluster", "kpsCount", "iif", "cf", "cfIif")
      .orderBy(desc("kpsCount"))


    savePath match {
      case Some(spath) => {
        join
          .saveAsParquetFile(spath+ConfImageRetrieval.CFIIF_FEAT_PATH)

        println("CfIif Features saved in: "+spath+ConfImageRetrieval.CFIIF_FEAT_PATH)
      }
      case None => ()
    }



    println(s"CfIif features (to Pivot and Vectorize): ${join.count}")
    join.show
    join


  }

}
