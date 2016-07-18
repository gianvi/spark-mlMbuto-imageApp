package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels;


import org.openimaj.image.FImage;
import org.openimaj.image.ImageProvider;
import org.openimaj.io.ReadWriteable;

import java.io.Serializable;


public interface ImageData extends ReadWriteable, ImageProvider<FImage>, Serializable {
    FImage getImage();

    String getType();

    String getPath();
}
