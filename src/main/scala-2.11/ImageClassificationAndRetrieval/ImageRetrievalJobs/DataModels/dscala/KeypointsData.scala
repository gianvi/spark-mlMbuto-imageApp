package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels.dscala

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by gian on 11/06/16.
  */
case class KeypointData(label: String, image: String, keypoint: Vector, cluster: Int)

