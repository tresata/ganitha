package com.tresata.ganitha.ml.classification

import com.twitter.scalding.Dsl._

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.JobConf
import org.apache.mahout.math.{ Vector => MahoutVector }
import breeze.linalg.{ Vector => BreezeVector }
import org.jblas.{ DoubleMatrix => JblasVector }
import org.saddle.{ Vec => SaddleVector }

import cascading.pipe.Pipe
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.{ TextDelimited, SequenceFile => CSequenceFile }

import com.twitter.scalding.{ Args, Job, SequenceFile, Csv }

import com.tresata.ganitha.util._
import com.tresata.ganitha.mahout._
import com.tresata.ganitha.mahout.Implicits._
import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.ml.util.VectorImplicits._

import org.scalatest.FunSpec

class GaussianNBSpec extends FunSpec {

  val job = new JobConf
  ScaldingUtils.setScaldingDefaults(job)

  describe("A Gaussian Naive-Bayes classifier") {

    it("should train and classify a set of MahoutVectors from a file") {
      // read training set vectors into Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBTrainingTestJob --hdfs --input %s --output %s --vectype mahout"
        .format("test/data/iris/iris_train.csv", "tmp/iris/gnb_model_mahout").split("\\s+"))

      // classify test set using Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBClassifyingTestJob --hdfs --input %s --output %s --model %s --vectype %s"
        .format("test/data/iris/iris_test.csv", "tmp/iris/gnb_output_mahout", "tmp/iris/gnb_model_mahout", "mahout").split("\\s+"))

      // verify >= 90% correctness
      val nbOutputTap = new Hfs(new CSequenceFile(), "tmp/iris/gnb_output_mahout").asInstanceOf[HadoopTap]
      val nbOutput = ScaldingUtils.toSeq[(String, MahoutVector, String)](nbOutputTap, job).map{ case(before, v, after) => (before, after) }
      val ratioCorrect = nbOutput.count{ case(trueLabel, nbLabel) => trueLabel == nbLabel } / nbOutput.size.toDouble
      println(ratioCorrect * 100 + "% predications correct")
      assert(ratioCorrect >= .9, ratioCorrect * 100 + "% correctness not above 90% threshold")
    }

    it("should train and classify a set of JblasVectors from a file") {
      // read training set vectors into Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBTrainingTestJob --hdfs --input %s --output %s --vectype jblas"
        .format("test/data/iris/iris_train.csv", "tmp/iris/gnb_model_jblas").split("\\s+"))

      // classify test set using Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBClassifyingTestJob --hdfs --input %s --output %s --model %s --vectype %s"
        .format("test/data/iris/iris_test.csv", "tmp/iris/gnb_output_jblas", "tmp/iris/gnb_model_jblas", "jblas").split("\\s+"))

      // verify >= 90% correctness
      val nbOutputTap = new Hfs(new CSequenceFile(), "tmp/iris/gnb_output_jblas").asInstanceOf[HadoopTap]
      val nbOutput = ScaldingUtils.toSeq[(String, JblasVector, String)](nbOutputTap, job).map{ case(before, v, after) => (before, after) }
      val ratioCorrect = nbOutput.count{ case(trueLabel, nbLabel) => trueLabel == nbLabel } / nbOutput.size.toDouble
      println(ratioCorrect * 100 + "% predications correct")
      assert(ratioCorrect >= .9, ratioCorrect * 100 + "% correctness not above 90% threshold")
    }

    it("should train and classify a set of BreezeVectors from a file") {
      pending
      // read training set vectors into Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBTrainingTestJob --hdfs --input %s --output %s --vectype breeze"
        .format("test/data/iris/iris_train.csv", "tmp/iris/gnb_model_breeze").split("\\s+"))

      // classify test set using Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBClassifyingTestJob --hdfs --input %s --output %s --model %s --vectype %s"
        .format("test/data/iris/iris_test.csv", "tmp/iris/gnb_output_breeze", "tmp/iris/gnb_model_breeze", "breeze").split("\\s+"))

      // verify >= 90% correctness
      val nbOutputTap = new Hfs(new CSequenceFile(), "tmp/iris/gnb_output_breeze").asInstanceOf[HadoopTap]
      val nbOutput = ScaldingUtils.toSeq[(String, BreezeVector[Double], String)](nbOutputTap, job).map{ case(before, v, after) => (before, after) }
      val ratioCorrect = nbOutput.count{ case(trueLabel, nbLabel) => trueLabel == nbLabel } / nbOutput.size.toDouble
      println(ratioCorrect * 100 + "% predications correct")
      assert(ratioCorrect >= .9, ratioCorrect * 100 + "% correctness not above 90% threshold")
    }

    it("should train and classify a set of SaddleVectors from a file") {
      // read training set vectors into Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBTrainingTestJob --hdfs --input %s --output %s --vectype saddle"
        .format("test/data/iris/iris_train.csv", "tmp/iris/gnb_model_saddle").split("\\s+"))

      // classify test set using Naive-Bayes model
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.classification.GaussianNBClassifyingTestJob --hdfs --input %s --output %s --model %s --vectype %s"
        .format("test/data/iris/iris_test.csv", "tmp/iris/gnb_output_saddle", "tmp/iris/gnb_model_saddle", "saddle").split("\\s+"))

      // verify >= 90% correctness
      val nbOutputTap = new Hfs(new CSequenceFile(), "tmp/iris/gnb_output_saddle").asInstanceOf[HadoopTap]
      val nbOutput = ScaldingUtils.toSeq[(String, SaddleVector[Double], String)](nbOutputTap, job).map{ case(before, v, after) => (before, after) }
      val ratioCorrect = nbOutput.count{ case(trueLabel, nbLabel) => trueLabel == nbLabel } / nbOutput.size.toDouble
      println(ratioCorrect * 100 + "% predications correct")
      assert(ratioCorrect >= .9, ratioCorrect * 100 + "% correctness not above 90% threshold")
    }

  }

}

object GaussianNBSpec {
  def jblasVectorizer(x1: Double, x2: Double, x3: Double, x4: Double) = new JblasVector(Array(x1, x2, x3, x4))
  def mahoutVectorizer(x1: Double, x2: Double, x3: Double, x4: Double) = RichVector(Array(x1, x2, x3, x4))
  def breezeVectorizer(x1: Double, x2: Double, x3: Double, x4: Double) = BreezeVector(x1, x2, x3, x4)
  def saddleVectorizer(x1: Double, x2: Double, x3: Double, x4: Double) = SaddleVector(x1, x2, x3, x4)
  def vectorizer(vectype: String): scala.Function4[Double, Double, Double, Double, _] = vectype match {
    case "jblas" => GaussianNBSpec.jblasVectorizer _
    case "mahout" => GaussianNBSpec.mahoutVectorizer _
    case "breeze" => GaussianNBSpec.breezeVectorizer _
    case "saddle" => GaussianNBSpec.saddleVectorizer _
  }
  def vectorHelper(vectype: String): DenseVectorHelper[_] = vectype match {
    case "jblas" => jblasVectorHelper
    case "mahout" => mahoutVectorHelper
    case "breeze" => breezeVectorHelper
    case "saddle" => saddleVectorHelper
  }
}

class GaussianNBTrainingTestJob(args: Args) extends Job(args) {
  def vectorizer = GaussianNBSpec.vectorizer(args("vectype"))
  val helper = GaussianNBSpec.vectorHelper(args("vectype"))
  val irisFeatures = ("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width")
  Csv(args("input"), skipHeader = true).read
    .map(irisFeatures -> 'features) { x: (Double, Double, Double, Double) => vectorizer(x._1, x._2, x._3, x._4) }
    .thenDo{ pipe: Pipe => GaussianNB.train(('Species, 'features))(pipe)(helper) }
    .write(SequenceFile(args("output"), ('label, 'pi, 'mu, 'sigma)))
}

class GaussianNBClassifyingTestJob(args: Args) extends Job(args) {
  def vectorizer = GaussianNBSpec.vectorizer(args("vectype"))
  val helper = GaussianNBSpec.vectorHelper(args("vectype"))
  val irisFeatures = ("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width")
  def nbModelTap = new Hfs(new CSequenceFile(('label, 'pi, 'mu, 'sigma)), args("model")).asInstanceOf[HadoopTap]
  val nbModel = GaussianNB.modelFromTap(nbModelTap)(helper)
  Csv(args("input"), skipHeader = true).read
    .rename('Species -> 'trueLabel)
    .map(irisFeatures -> 'features) { x: (Double, Double, Double, Double) => vectorizer(x._1, x._2, x._3, x._4) }
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
