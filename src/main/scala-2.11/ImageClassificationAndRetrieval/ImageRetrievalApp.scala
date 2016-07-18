package ImageClassificationAndRetrieval

import ImageClassificationAndRetrieval.ImageRetrievalJobs.ConfImageRetrieval
import ImageClassificationAndRetrieval.ImageRetrievalJobs.FeatEngineering.{ClusterVectorRDDPivoter, CfIifTransformer, KMQuantiser, KPExtractor}
import ImageClassificationAndRetrieval.ImageRetrievalJobs.KNNModels.{Cross, Base, Naive}
import ImageClassificationAndRetrieval.ImageRetrievalJobs.Readers.ImageLoader
import core.SparkJobApp

/**
  * Created by gian on 05/05/16.
  */
object ImageRetrievalApp extends SparkJobApp {

  //Ogni step puo essere caricato con dati dal precedente o da un path! -> gestire meglio le options!
  implicit val TIMESTAMP = System.currentTimeMillis

  val selectedPipeline = "ImageRetrievalApp"
  val selectedKNNClassifier = ConfImageRetrieval.KNNClassifier.getOrElse("base", "")
  val selectedKMQuantiser = ConfImageRetrieval.KMquantiser.getOrElse("SparkMLLib", "")


  selectedPipeline match {

    case "ImageRetrievalApp"  => run(job = for {

      //null save path to read pre-loaded images
      images <- new ImageLoader(
        preloadPath = Option.empty[String],
        loadPath = ConfImageRetrieval.SUBPATH_TO_LOAD,
        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/2ImageLoader/imagedata_${TIMESTAMP}.txt")
      )

      keypoints <- new KPExtractor(
        images = Option(images),
        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/3KPExtractor/keypoints_${TIMESTAMP}.txt")
      )

      kpsWithCluster <- new KMQuantiser(
        keypoints = keypoints,
        mode = selectedKMQuantiser,
        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/4KMQuantiser/${TIMESTAMP}/${selectedKMQuantiser}/"),  //will save features and model
        loadPath = Option.empty[String]
      )

      cfIif <- new CfIifTransformer(
        data = Option(kpsWithCluster),
        loadPath = Option.empty[String],
        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/5CFIIFTransformer/${TIMESTAMP}/")
      )

      clusterVectorDictionary <- new ClusterVectorRDDPivoter(
        data = Option(cfIif),
        loadPath = Option.empty[String],
        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/6ClusterVectorRDDPivoter/${TIMESTAMP}/")
      )

    //TODO choose model

//      _ <- new Naive(
//        data = Option(clusterVectorDictionary),
//        loadPath = Option.empty[String],
//        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/7classification/${selectedKNNClassifier}/${TIMESTAMP}/")
//      )

      _ <- new Base(
        data = Option(clusterVectorDictionary),
        loadPath = Option.empty[String],//("/home/gian/Workspace/Actual/spark-ml-mbuto/data/ImageRetrievalApp/6ClusterVectorRDDPivoter/1465767534086/ClusterVector"),//Option("/home/gian/Workspace/inGit/spark-poc/data/imagePipeline/4ClusterVectorRDDPivoter/1465349000385/features/clusterVector.parquet/"),Option("/home/gian/Workspace/inGit/spark-poc/data/imagePipeline/4ClusterVectorRDDPivoter/1465346943261/features/clusterVector.parquet"),//"/home/gian/Workspace/inGit/spark-poc/data/imagePipeline/CFIIF/1465161126096/forKNNAfterrddpivoting/myFeatures.parquet"// data/imagePipeline/CFIIF/withLabel/myFeatures.parquet"
        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/7classification/${TIMESTAMP}/${selectedKNNClassifier}/")
      )

//      _ <- new Cross(
//        data = Option(clusterVectorDictionary),
//        loadPath = Option.empty[String],
//        savePath = Option(s"${ConfImageRetrieval.SUBPATH_TO_SAVE}/7classification/${TIMESTAMP}/${selectedKNNClassifier}/")
//      )



    } yield ())

    case "" =>  println("No pipelines selected!")

    case _ => println(s"This pipeline ${selectedPipeline.toUpperCase} is not yet implemented!")
  }


}