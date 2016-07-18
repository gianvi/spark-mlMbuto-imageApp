package ImageClassificationAndRetrieval.ImageRetrievalJobs

/**
  * Created by gian on 08/05/16.
  */

object ConfImageRetrieval {

  final val TIMESTAMP = System.currentTimeMillis

  val KMquantiser = Map("SparkML" -> "SparkML", "SparkMLLib" -> "SparkMLLib", "OpenImaj" -> null) //to use openImaj need to cast to dataframe

  val KNNClassifier = Map("naive"-> "naive","base"->"base", "cross"->"cross")

  val NUM_CLASSES = 3

  val NUM_TRAINING_IMAGES_PER_CLASS = 15

  val CORPUS = NUM_TRAINING_IMAGES_PER_CLASS * NUM_CLASSES

  val NUM_TEST_IMAGES_PER_CLASS = 5

  val SUBPATH_TO_SAVE = "data/ImageRetrievalApp"

  val SUBPATH_TO_LOAD = "data/ImageRetrievalApp/1imagecl_sample/train/"

  //indica la percentuale sul num complessivo dei kps
  val KM_PERCENTAGE_CLUSTER = 1D / 10000D

  val KM_NUM_ITERATIONS = 100

  val KM_FEAT_PATH = "KpsAndCluster/"

  val KM_MODEL_PATH = "model/"

  val CFIIF_FEAT_PATH = "CfIifFeatures/"

  val CV_FEAT_PATH = "ClusterVector/"


}