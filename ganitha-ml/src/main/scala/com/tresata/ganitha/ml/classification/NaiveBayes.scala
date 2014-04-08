package com.tresata.ganitha.ml.classification

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.JobConf

import cascading.flow.hadoop.HadoopFlowConnector
import cascading.pipe.Pipe
import cascading.tuple.Fields

import com.twitter.scalding.Dsl._

import com.tresata.ganitha.util._
import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.mahout.Implicits._
import com.tresata.ganitha.mahout.VectorSerializer
import com.twitter.scalding.FunctionImplicits._

import org.slf4j.LoggerFactory

abstract class NaiveBayesModel[VecType <: AnyRef] extends ClassificationModel {
  /**
    * Returns the label given to the input vector by the maximum a posteriori probability.
    *
    * @param dataPt VecType representing the data point to be classified
    * @return String representing the label given to the data point by the classification model
    */
  def predict(dataPt: VecType): String

  /**
    * Returns a Pipe containing the labels given to each vector in the input as determined by the
    * classification model.
    *
    * @param fs Fields containing the features vector and the newly assigned label
    * @param data Pipe containing the features vectors to classify
    * @return Pipe containing the features vectors along with their assigned label
    */
  def classify(fs: (Fields, Fields) = ('features, 'label))(data: Pipe): Pipe
}

/**
  * Model for a Multinomial Naive-Bayes Classifier.
  *
  * @constructor Creates a new MultinomialNBModel using the given prior probabilities and feature weights vectors
  * @tparam VecType type of the vectors being classified
  * @param pi Seq[Named[Double]] containing labels and their prior probabilities
  * @param theta Seq[Named[VecType]] containing labels and their log feature weights vectors
  */
class MultinomialNBModel[VecType <: AnyRef](val pi: Seq[Named[Double]], val theta: Seq[Named[VecType]])
(implicit helper: VectorHelper[VecType]) extends NaiveBayesModel[VecType] {
  assert(pi.size == theta.size, "Number of prior probabilities must equal number of weight vectors")

  val priors = pi.toMap
  val weights = theta.toMap
  val labels = pi.map(_._1).toSet

  import MultinomialNBModel._
  logger.info("Multinomial Naive-Bayes Model:\n{}", toString)

  override def toString() = "Pi:\n---\n" + pi.mkString("\n") + "\nTheta:\n------\n" + theta.mkString("\n")

  def predict(dataPt: VecType): String = labels
    .map{ label => label -> (priors(label) + helper.dot(weights(label), dataPt)) }
    .maxBy(_._2)._1

  def classify(fs: (Fields, Fields) = ('features, 'label))(data: Pipe): Pipe = {
    assert(fs.size == 2, "Must specify exactly two Field names for the labels and feature vectors")
    data.map(fs) { vector: VecType => predict(vector) }
  }
}

object MultinomialNBModel {
  private val logger = LoggerFactory.getLogger(MultinomialNBModel.getClass)
}

/**
  * Model for a Bernoulli Naive-Bayes Classifier.
  *
  * @constructor Creates a new BernoulliNBModel using the given prior probabilities and feature weights vectors
  * @tparam VecType type of the vectors being classified
  * @param pi Seq[Named[Double]] containing labels and their prior probabilities
  * @param theta Seq[Named[VecType]] containing labels and their log feature weights vectors
  */
class BernoulliNBModel[VecType <: AnyRef](val pi: Seq[Named[Double]], val theta: Seq[Named[VecType]])
(implicit helper: DenseVectorHelper[VecType]) extends NaiveBayesModel[VecType] {
  assert(pi.size == theta.size, "Number of prior probabilities must equal number of weight vectors")
  assert(theta.forall{ case(l, v) => helper.iterator(v).count(_ > 1.0) == 0 }, "weights vector values should be less than 1.0")

  val priors = pi.toMap
  val weights = theta.toMap
  val weights_1minus = weights.mapValues{ v => helper.map(v, {x => math.log(1 - math.exp(x))}) }
  val labels = pi.map(_._1).toSet

  import BernoulliNBModel._
  logger.info("Bernoulli Naive-Bayes Model:\n{}", toString)

  override def toString() = "Pi:\n---\n" + pi.mkString("\n") + "\nTheta:\n------\n" + theta.mkString("\n")

  def predict(dataPt: VecType): String = labels
    .map{ label => label -> (priors(label) +
      helper.dot(weights(label), dataPt) +
      helper.dot(weights_1minus(label), helper.map(dataPt, {x => 1 - x})))
    }.maxBy(_._2)._1

  def classify(fs: (Fields, Fields) = ('features, 'label))(data: Pipe): Pipe = {
    assert(fs.size == 2, "Must specify exactly two Field names for the labels and feature vectors")
    data.map(fs) { vector: VecType => predict(vector) }
  }
}

object BernoulliNBModel {
  private val logger = LoggerFactory.getLogger(BernoulliNBModel.getClass)
}

/**
  * Model for a Gaussian Naive-Bayes Classifier.
  *
  * @constructor Creates a new GaussianNBModel using the given priors, means, and variances
  * @tparam VecType type of the vectors being classified
  * @param pi Seq[Named[Double]] containing labels and their prior probabilities
  * @param mu Seq[Named[VecType]] containing labels and their mean vectors
  * @param sigma Seq[Named[VecType]] containing labels and their variance vectors
  */
class GaussianNBModel[VecType <: AnyRef](val pi: Seq[Named[Double]], val mu: Seq[Named[VecType]], val sigma: Seq[Named[VecType]])
(implicit helper: DenseVectorHelper[VecType]) extends NaiveBayesModel[VecType] {
  assert(pi.size == mu.size, "Number of prior probabilities must equal number of mu vectors")
  assert(pi.size == sigma.size, "Number of prior probabilities must equal number of sigma vectors")

  val priors = pi.toMap.mapValues(math.log(_))
  val means = mu.toMap
  val variances = sigma.toMap
  val labels = pi.map(_._1).toSet

  import GaussianNBModel._
  logger.info("Gaussian Naive-Bayes Model:\n{}", toString)

  override def toString() = "Pi:\n---\n" + pi.mkString("\n") +
    "\nMu:\n------\n" + mu.mkString("\n") +
    "\nSigma:\n------\n" + sigma.mkString("\n")

  private def gaussianLogProb(mean: Double, vari: Double)(x: Double) =
    - math.pow(x - mean, 2) / (2  * vari) - math.log(math.sqrt(2 * math.Pi * vari))

  private def gaussianLogProbSum(pi_l: Double, mu_l: VecType, sigma_l: VecType)(v: VecType) =
    pi_l + helper.iterator(mu_l).zip(helper.iterator(sigma_l)).zip(helper.iterator(v)).map {
      case((mean, vari), x) => gaussianLogProb(mean, vari)(x)
    }.sum

  def predict(dataPt: VecType): String = labels
  .map{ label => label -> gaussianLogProbSum(priors(label), means(label), variances(label))(dataPt) }
  .maxBy(_._2)._1

  def classify(fs: (Fields, Fields) = ('features, 'label))(data: Pipe): Pipe = {
    assert(fs.size == 2, "Must specify exactly two Field names for the labels and feature vectors")
    data.map(fs) { vector: VecType => predict(vector) }
  }
}

object GaussianNBModel {
  private val logger = LoggerFactory.getLogger(GaussianNBModel.getClass)
}

/**
  * Multinomial Naive-Bayes for handling features vectors that represent frequencies with which certain events have
  * been generated by a multinomial distribution. This variant is typically used for document classification, where
  * words (features) have an associated term-frequency, or TF-IDF score, for example.
  */
object MultinomialNB {
  /**
    * Returns a Pipe containing the prior probability and feature weights vector for each label, which can
    * then be read into a classification model. The input pipe should contain two fields containing data points
    * from a training set and the assigned labels. Note that the normalizing constant is omitted in the calculation
    * of the prior (log) probabilities, `pi`, as it is the same for all classes, and will therefore not affect the
    * maximum a posteriori class. The `theta` field contains the logs of the class conditional probabilities.
    *
    * @tparam VecType type of the vectors being read in from the training Pipe
    * @param fs Fields containing the label and the features vector of the data point
    * @param lambda Double representing the Laplace smoothing parameter
    * @param data Pipe containing the labeled vectors in the training set
    * @return Pipe containing the pi value and theta vector for each label
    */
  def train[VecType](fs: Fields = ('label, 'features), lambda: Double = 1.0)(data: Pipe)
  (implicit helper: VectorHelper[VecType]): Pipe = {
    assert(fs.size == 2, "Must specify exactly two Field names for the labels and feature vectors")
    assert(lambda >= 0.0, "Smoothing parameter, lambda, must be a nonnegative value")

    val aggregates = data
      .project(fs)
      .rename(fs -> ('label, 'features))
      .groupBy('label) { _
        .size('classCount)
        .reduce[VecType]('features -> 'featureSum){ (v1, v2) => helper.plus(v1, v2) }
      }
      .project('label, 'classCount, 'featureSum)

    aggregates
      .map('classCount -> 'pi) { classCount: Long => math.log(classCount + lambda) }
      .map('featureSum -> 'theta) { fSum: VecType =>
        helper.map(fSum, { f => math.log(f + lambda) - math.log(helper.sum(fSum) + helper.size(fSum) * lambda) })
      }
      .project('label, 'pi, 'theta)
  }

  /**
    * Returns a Multinomial Naive-Bayes model based created from the prior probabilities and feature weights vectors stored
    * in the given HadoopTap.
    *
    * @tparam VecType type of the vectors being read in from the training Pipe
    * @param modelTap HadoopTap containing the pi value and theta vector for each label
    * @param fs Fields containing the labels, prior probabilities, and feature weights vectors
    * @param jobConf JobConf containing configurations
    * @return MultinomialNBModel containing prior probabilities and feature weights vectors for each label
    */
  def modelFromTap[VecType <: AnyRef](modelTap: HadoopTap, fs: Fields = ('label, 'pi, 'theta), jobConf: JobConf = new JobConf)
  (implicit helper: VectorHelper[VecType]): MultinomialNBModel[VecType] = {
    assert(fs.size == 3, "Must specify exactly three Field names for the labels, prior probabilities and feature weights vectors")

    val job = new JobConf(jobConf)
    ScaldingUtils.setScaldingDefaults(job)
    VectorSerializer.register(job)
    val nbModel = ScaldingUtils.toSeq[(String, Double, VecType)](modelTap, job)
    val pi: Seq[Named[Double]] = nbModel.map{ case(label, pi, theta) => (label, pi) }
    val theta: Seq[Named[VecType]] = nbModel.map{ case(label, pi, theta) => (label, theta) }

    new MultinomialNBModel[VecType](pi, theta)(helper)
  }
}

/**
  * Bernoulli Naive-Bayes for handling features vectors that represent 1 or 0 values. This variant is typically used
  * for document classification, where the word set is small. In Bernoulli Naive-Bayes, the vectors represent binary
  * occurence features, and the absence of terms is factored into the model.
  */
object BernoulliNB {
  /**
    * Returns a Pipe containing the prior probability and feature weights vector for each label, which can
    * then be read into a classification model. The input pipe should contain two fields containing data points
    * from a training set and the assigned labels. Note that the normalizing constant is omitted in the calculation
    * of the prior (log) probabilities, `pi`, as it is the same for all classes, and will therefore not affect the
    * maximum a posteriori class. The `theta` field contains the logs of the class conditional probabilities.
    *
    * @tparam VecType type of the vectors being read in from the training Pipe
    * @param fs Fields containing the label and the features vector of the data point
    * @param lambda Double representing the Laplace smoothing parameter
    * @param data Pipe containing the labeled vectors in the training set
    * @return Pipe containing the pi value and theta vector for each label
    */
  def train[VecType](fs: Fields = ('label, 'features), lambda: Double = 1.0)(data: Pipe)
  (implicit helper: DenseVectorHelper[VecType]): Pipe = {
    assert(fs.size == 2, "Must specify exactly two Field names for the labels and feature vectors")
    assert(lambda >= 0.0, "Smoothing parameter, lambda, must be a nonnegative value")

    val aggregates = data
      .project(fs)
      .rename(fs -> ('label, 'features))
      .groupBy('label) { _
        .size('classCount)
        .reduce[VecType]('features -> 'featureSum){ (v1, v2) => helper.plus(v1, v2) }
      }
      .project('label, 'classCount, 'featureSum)

    aggregates
      .map('classCount -> 'pi) { classCount: Long => math.log(classCount + lambda) }
      .map('featureSum -> 'theta) { fSum: VecType =>
        helper.map(fSum, { f => math.log(f + lambda) - math.log(helper.sum(fSum) + helper.size(fSum) * lambda) })
      }
      .project('label, 'pi, 'theta)
  }

  /**
    * Returns a Bernoulli Naive-Bayes model based created from the prior probabilities and feature weights vectors stored in
    * the given HadoopTap.
    *
    * @tparam VecType type of the vectors being read in from the training Pipe
    * @param modelTap HadoopTap containing the pi value and theta vector for each label
    * @param fs Fields containing the labels, prior probabilities, and feature weights vectors
    * @param jobConf JobConf containing configurations
    * @return BernoulliNBModel containing prior probabilities and feature weights vectors for each label
    */
  def modelFromTap[VecType <: AnyRef](modelTap: HadoopTap, fs: Fields = ('label, 'pi, 'theta), jobConf: JobConf = new JobConf)
  (implicit helper: DenseVectorHelper[VecType]): BernoulliNBModel[VecType] = {
    assert(fs.size == 3, "Must specify exactly three Field names for the labels, prior probabilities and feature weights vectors")

    val job = new JobConf(jobConf)
    ScaldingUtils.setScaldingDefaults(job)
    VectorSerializer.register(job)
    val nbModel = ScaldingUtils.toSeq[(String, Double, VecType)](modelTap, job)
    val pi: Seq[Named[Double]] = nbModel.map{ case(label, pi, theta) => (label, pi) }
    val theta: Seq[Named[VecType]] = nbModel.map{ case(label, pi, theta) => (label, theta) }

    new BernoulliNBModel[VecType](pi, theta)(helper)
  }
}

/**
  * Gaussian Naive-Bayes for handling continuous data where the values associated with each attribute are distributed
  * along a Gaussian distribution within a class/label.
  */
object GaussianNB {
  /**
    * Returns a Pipe containing the prior probability and mean/variance vectors for each label, which can then
    * be read into a classification model. The input pipe should contain two fields containing data points from
    * a training set and the assigned labels.
    *
    * @tparam VecType type of the vectors being read in from the training Pipe
    * @param fs Fields containing the label and the features vector of the data point
    * @param data Pipe containing the labeled vectors in the training set
    * @return Pipe containing the pi value and mu/sigma vectors for each label
    */
  def train[VecType](fs: Fields = ('label, 'features))(data: Pipe)
  (implicit helper: DenseVectorHelper[VecType]): Pipe = {
    assert(fs.size == 2, "Must specify exactly two Field names for the labels and feature vectors")

    val moments = data
      .project(fs)
      .rename(fs -> ('label, 'features))
      .map('features -> 'featuresSq) { v: VecType => helper.map(v, {x => x * x}) }
      .groupBy('label) { _
        .size('classCount)
        .reduce[VecType]('features -> 'featuresSum){ (v1, v2) => helper.plus(v1, v2) }
        .reduce[VecType]('featuresSq -> 'featuresSqSum){ (v1, v2) => helper.plus(v1, v2) }
      }
      .project('label, 'classCount, 'featuresSum, 'featuresSqSum)

    moments
      .map('classCount -> 'pi) { classCount: Long => math.log(classCount) }
      .map(('classCount, 'featuresSum) -> 'mu) { (classCount: Long, fSum: VecType) => helper.divBy(fSum, classCount) }
      .map(('classCount, 'featuresSqSum, 'mu) -> 'sigma) {
        (classCount: Long, fSqSum: VecType, mu: VecType) =>
          helper.plus(helper.divBy(fSqSum, classCount), helper.map(mu, {x => - x * x}))
      }
      .project('label, 'pi, 'mu, 'sigma)
  }

  /**
    * Returns a Gaussian Naive-Bayes model based created from the prior probabilities and mean/variance vectors stored
    * in the given HadoopTap.
    *
    * @tparam VecType type of the vectors being read in from the training Pipe
    * @param modelTap HadoopTap containing the pi value and mu/sigma vectors for each label
    * @param fs Fields containing the labels, prior probabilities, and mean/variance vectors
    * @param jobConf JobConf containing configurations
    * @return GaussianNBModel containing prior probabilities and mean/variance vectors for each label
    */
  def modelFromTap[VecType <: AnyRef](modelTap: HadoopTap, fs: Fields = ('label, 'pi, 'mu, 'sigma), jobConf: JobConf = new JobConf)
  (implicit helper: DenseVectorHelper[VecType]): GaussianNBModel[VecType] = {
    assert(fs.size == 4, "Must specify exactly four Field names for the labels, prior probabilities and mean/variance vectors")

    val job = new JobConf(jobConf)
    ScaldingUtils.setScaldingDefaults(job)
    VectorSerializer.register(job)
    val nbModel = ScaldingUtils.toSeq[(String, Double, VecType, VecType)](modelTap, job)
    val pi: Seq[Named[Double]] = nbModel.map{ case(label, pi, mu, sigma) => (label, pi) }
    val mu: Seq[Named[VecType]] = nbModel.map{ case(label, pi, mu, sigma) => (label, mu) }
    val sigma: Seq[Named[VecType]] = nbModel.map{ case(label, pi, mu, sigma) => (label, sigma) }

    new GaussianNBModel[VecType](pi, mu, sigma)(helper)
  }
}
