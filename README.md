# spark-mlMbuto-imageApp
Image classification and retrieval app on spark - ML Mbuto app

This project contains an ML pipeline to 
 - process, 
 - engineer, 
 - classify 
 - and retrieve 
images with Spark.

It's a proof of concept with example showing how to use the Mbuto JobRunner and pipelines abstractions. 

**Slides:** [image classification and retrieval on spark](http://www.slideshare.net/gianvitosiciliano/image-classification-and-retrieval-on-spark)

It consist of a semi-advanced image pipeline with the following steps: 

- image loader: load real labeled images of 3 categories (car, bycicle, motorbike)
- keypoints extractor: use the SIFT extractor to get keypoints features vector fro image (openImaj)
- k-means quantiser: clustering keypoints to obtain a centroids dictionary (for search and retrievial)
- CF IIF transformer: (see the slides) features creation, cluster weighted vector
- ClusterVectorPivoter: create the dictionary for classification and retrieval
- KNN: there are different implementations of this model 
   * Naive: brute force
   * KDtree: openImaj version
   * Base: metric and still tree
   * PCA: base + pca features reduction
   * Normalized: base + ml features normalization
   * Cross: base + ml cross validation

For every question and suggestion, just contact me!

### Evaluation and analysis notebook

![Create and load image data](/zeppelin-notebook/zep1.tiff?raw=true)
![Data distribution](/zeppelin-notebook/zep2.tiff?raw=true)
![Model accuracy](/zeppelin-notebook/zep3.tiff?raw=true)
![Confusion matrix](/zeppelin-notebook/zep4.tiff?raw=true)
![Overall Evaluation](/zeppelin-notebook/zep5.tiff?raw=true)






