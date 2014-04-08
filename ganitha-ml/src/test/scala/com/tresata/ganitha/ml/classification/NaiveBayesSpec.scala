package com.tresata.ganitha.ml.classification

import com.twitter.scalding.Dsl._

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.JobConf
import org.apache.mahout.math.{ Vector => MahoutVector }

import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.{ TextDelimited, SequenceFile => CSequenceFile }

import com.twitter.scalding.{ Args, Job, SequenceFile, Csv }

import com.tresata.ganitha.util._
import com.tresata.ganitha.mahout._
import com.tresata.ganitha.mahout.Implicits._
import com.tresata.ganitha.mahout.VectorSerializer
import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.ml.util.VectorImplicits._

import org.scalatest.FunSpec

class GaussianNBSpec extends FunSpec {

  describe("A Gaussian Naive-Bayes classifier") {

    it("should train and classify a set of MahoutVectors from a file") {
      // read training set vectors into Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBTrainingTestJob --hdfs --input %s --output %s"
        .format("test/data/iris/iris_train.csv", "tmp/iris/nb_model").split("\\s+"))

      // classify test set using Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBClassifyingTestJob --hdfs --input %s --output %s"
        .format("test/data/iris/iris_test.csv", "tmp/iris/nb_output").split("\\s+"))

      // verify >= 90% correctness
      val job = new JobConf
      ScaldingUtils.setScaldingDefaults(job)
      VectorSerializer.register(job)
      val nbOutputTap = new Hfs(new CSequenceFile(), "tmp/iris/nb_output").asInstanceOf[HadoopTap]
      val nbOutput = ScaldingUtils.toSeq[(String, MahoutVector, String)](nbOutputTap, job).map{ case(before, v, after) => (before, after) }
      val ratioCorrect = nbOutput.count{ case(trueLabel, nbLabel) => trueLabel == nbLabel } / nbOutput.size.toDouble
      println(ratioCorrect * 100 + "% predications correct")
      assert(ratioCorrect >= .9, ratioCorrect * 100 + "% correctness not above 90% threshold")
    }

  }

}

class GaussianNBTrainingTestJob(args: Args) extends Job(args) {
  val irisFeatures = ("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width")
  Csv(args("input"), skipHeader = true).read
    .map(irisFeatures -> 'features) { x: (Double, Double, Double, Double) => RichVector(Array(x._1, x._2, x._3, x._4)) }
    .thenDo{ GaussianNB.train[MahoutVector](('Species, 'features)) }
    .write(SequenceFile(args("output"), ('label, 'pi, 'mu, 'sigma)))
}

class GaussianNBClassifyingTestJob(args: Args) extends Job(args) {
  val irisFeatures = ("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width")
  val nbModelTap = new Hfs(new CSequenceFile(('label, 'pi, 'mu, 'sigma)), "tmp/iris/nb_model").asInstanceOf[HadoopTap]
  val nbModel = GaussianNB.modelFromTap[MahoutVector](nbModelTap)
  Csv(args("input"), skipHeader = true).read
    .rename('Species -> 'trueLabel)
    .map(irisFeatures -> 'features) { x: (Double, Double, Double, Double) => RichVector(Array(x._1, x._2, x._3, x._4)) }
    .thenDo{ nbModel.classify('features -> 'nbLabel) }
    .project('trueLabel, 'features, 'nbLabel)
    .write(SequenceFile(args("output"), ('trueLabel, 'features, 'nbLabel)))
}

class MultinomialNBSpec extends FunSpec {

  describe("A Multinomial Naive-Bayes classifier") {

    it("should train and classify a set of MahoutVectors from a file") {
      pending
    }

  }

}

class BernoulliNBSpec extends FunSpec {

  describe("A Bernoulli Naive-Bayes classifier") {

    it("should train and classify a set of MahoutVectors from a file") {
      pending
    }

  }

}
