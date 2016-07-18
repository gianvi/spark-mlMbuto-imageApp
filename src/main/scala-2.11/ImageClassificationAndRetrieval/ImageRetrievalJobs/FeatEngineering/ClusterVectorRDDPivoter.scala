package ImageClassificationAndRetrieval.ImageRetrievalJobs.FeatEngineering

import ImageClassificationAndRetrieval.ImageRetrievalJobs.ConfImageRetrieval
import core.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
  * Created by gian on 03/04/16.
  *
  *
  *  This Pivot sample is based on the 5th answer given on:
  *  http://stackoverflows.xyz/questions/30260015/reshaping-pivoting-data-in-spark-rdd-and-or-spark-dataframes
  *  The answer above was written in Python, which I don't know very well.
  *  To help with other aspects of translating the Python sample I used these references:
  *  http://codingjunkie.net/spark-agr-by-key/
  *  http://scala4fun.tumblr.com/post/84792374567/mergemaps
  *  http://alvinalexander.com/scala/how-sort-scala-sequences-seq-list-array-buffer-vector-ordering-ordered
  *
  *
  *  TODO implement version with Combiner, and immutableMap
  */

class ClusterVectorRDDPivoter(data: Option[DataFrame], loadPath: Option[String], savePath: Option[String])
  extends SparkJob[DataFrame]
    with Serializable {


  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    import scala.collection._

    val features = data match {
      case None => {
        sqlContext
          .read
          .load(loadPath.get)
      }

      case Some(d) => d
    }

    features.show()
    features.printSchema()

    val featuresRDD = features.rdd.map{ x =>
      (x.getAs[String]("image"),
        x.getAs[String]("label"),
        x.getAs[Int]("cluster").toString,
        x.getAs[Double]("cfIif"))
    }

    val krows: RDD[(String, Row)] = featuresRDD
      .map(t => Row.fromSeq(t.productIterator.toList))
      .keyBy(_(0).toString)

    def seqPivot(m: mutable.Map[String, Any], r: Row): mutable.Map[String, Any] = {
      m += (r(2).toString -> r(3))
      m += ("label" -> r(1))
      m
    }

    def cmbPivot(m1:mutable.Map[String, Any], m2:mutable.Map[String, Any]): mutable.Map[String, Any] = {
      m1 ++= m2
      m1
    }

    val pivoted: RDD[(String, mutable.Map[String, Any])] = krows.aggregateByKey(mutable.Map.empty[String, Any])(seqPivot, cmbPivot)

    val orderedColnames: scala.Seq[String] = pivoted.values.map(v => v.keys.toSet).reduce((s, t) => s.union(t)).toSeq.sortWith(_ < _)

    val labelFieldMustBetheLastOne = orderedColnames.takeRight(1).head
    assert(labelFieldMustBetheLastOne == "label")

    val schema: StructType = StructType(
        List(StructField("image", StringType, true))
        ++
        (for (c <- orderedColnames if c != "label") yield StructField(c, DoubleType, true))
        ++
        List(StructField("label", StringType, true))
    )

    val keyedRows = pivoted.map{ case (image:String, lcMap:mutable.Map[String, Any]) =>
      List(image) ++ (for (c <- orderedColnames) yield lcMap.getOrElse(c, null))
    }.map(row => Row.fromSeq(row))

    val result = sqlContext
      .createDataFrame(keyedRows, schema)
      .na.fill(0D)

    savePath match {
      case Some(spath) => {
        result
          .saveAsParquetFile(spath+ConfImageRetrieval.CV_FEAT_PATH)

        println("ClusterVector features (null fixed) saved in: "+spath+ConfImageRetrieval.CV_FEAT_PATH)
      }
      case None => ()
    }

    println(s"RDD pivoter produced ${result.count} cluster vector weighted with cfIif of that cluster (x that image in a key!)")
    result.show
    result
  }
}
