package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels;


import org.openimaj.feature.DoubleFV;


public interface FVData extends ImageData {
    DoubleFV getFeatureVector();
}
