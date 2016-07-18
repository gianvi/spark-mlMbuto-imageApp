package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels;

import org.openimaj.feature.local.list.LocalFeatureList;
import org.openimaj.image.feature.dense.gradient.dsift.ByteDSIFTKeypoint;

public interface KPData extends ImageData {
    LocalFeatureList<ByteDSIFTKeypoint> getKeypoints();
}
