package com.tresata.ganitha.ml.classification

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.Dsl._

trait ClassificationModel extends Serializable {
  /**
    * Classifies the data points using the model trained.
    *
    * @param fs Fields representing the features vectors and classified labels
    * @param data Pipe containing features vectors to classify
    * @return Pipe containing label field predicted by model
    */
  def classify(fs: (Fields, Fields) = ('features, 'label))(data: Pipe): Pipe
}
