package com.tresata.ganitha.ml.clustering

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

import cascading.flow.hadoop.{ HadoopFlowConnector, HadoopFlowProcess }
import cascading.pipe.Pipe
import cascading.scheme.hadoop.SequenceFile
import cascading.tap.hadoop.Hfs
import cascading.tap.SinkMode

import com.twitter.scalding.Dsl._

import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.ml.util.VectorImplicits._
import com.tresata.ganitha.util._
import com.tresata.ganitha.util.FunctionImplicits._ // change to com.twitter.scalding.FunctionImplicits

import org.slf4j.{ Logger, LoggerFactory }

/**
  * Provides the scalding classes used for clustering algorithms.
  *
  * References:
  *
  * K-Means:
  * Lloyd, S., "Least squares quantization in PCM," IEEE Trans. Information Theory, 28(2):129-137, 1982.
  *
  * K-Means++:
  * Arthur, D. and Vassilvitskii, S. (2007). "k-means++: the advantages of careful seeding".
  * Proc. ACM-SIAM Symp. Discrete Algorithms. pp. 1027–1035.
  *
  * K-Means||:
  * Bahmani, B. et al. (2012). "Scalable k-means++". Proceedings of the VLDB Endowment, 5(7), 622-633.
  */
package clustering {}

object `package` {
  private val logger = LoggerFactory.getLogger("com.tresata.ganitha.ml.clustering")
  val IDS_PER_MAPPER = 10000
  val NUM_MAPPERS = 40

  /**
    * Returns a Tuple of two Ints representing the number of clusters in the first sequence that are absent in
    * the second sequence, and the number that remain. Note that the cluster sequences are converted to Maps in the
    * beginning of the function (in order to iterate through the key values to compare vectors).
    *
    * @tparam VecType type of incoming vector objects
    * @param clSeq1 Seq of named clusters to check for changes against
    * @param clSeq2 Seq of named clusters to compare to the first sequence
    * @param distFn Function that returns the distance between two clusters
    * @param epsilon Double representing the minimum distance necessary to establish equality of clusters
    * @param logger Logger to use for message logging (optional)
    * @return Tuple of two Ints representing the number of clusters in clSeq1 that are absent in clSeq2, and
    *         the number of clusters that are present
    */
  def clusterChanges[VecType](
    clSeq1: Seq[Named[VecType]], clSeq2: Seq[Named[VecType]],
    distFn: (VecType, VecType) => Double, epsilon: Double
    ): (Int, Int) = {
    var changed, unChanged = 0
    val clMap1 = clSeq1.toMap
    val clMap2 = clSeq2.toMap

    for ((id, vector) <- clMap2) {
      val delta = if (clMap1.contains(id)) distFn(clMap1(id), clMap2(id)) else Double.PositiveInfinity
      logger.debug("delta for cluster {}: {}", id, delta)
      if (delta > epsilon) changed += 1 else unChanged += 1
    }

    (changed, unChanged)
  }

  /**
    * Returns true if the given vector is within the epsilon-neighborhood of at least one of the given clusters.
    *
    * @tparam VecType type of incoming vector objects
    * @param clusterSeq Seq of current cluster vectors
    * @param distFn Function that returns the distance between two vectors
    * @param epsilon Double representing the minimum distance necessary to establish equality of vectors
    * @return true when the given vector is within the epsilon-neighborhood of at least one of the clusters in clusterSeq
    */
  def isInEpsilonNeighborhood[VecType](
    clusterSeq: Seq[VecType], cluster: VecType,
    distFn: (VecType, VecType) => Double, epsilon: Double
  ): Boolean = clusterSeq.exists{ distFn(cluster, _) < epsilon }

  /**
    * Returns a random subset of size k from the given Tap of named vectors.
    *
    * @param sourceTap HadoopTap containing the full set of vectors
    * @param k Int representing the number of random vectors to emit
    * @param fieldsIn Tuple containing the id and vector fields for incoming vectors
    * @param fieldsOut Tuple containing the outgoing id and vector field names
    * @param job JobConf containing the Path for output and the serialization settings
    * @return HadoopTap containing the set of k random vectors
    */
  def randomKSubset(
    sourceTap: HadoopTap, k: Int,
    fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol),
    job: JobConf = new JobConf): HadoopTap = {

    val (idIn, vectorIn) = fieldsIn
    val (idOut, vectorOut) = fieldsOut

    val tempTap = new TmpHfs(job)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)
    val randSubsetPath = new Path(CascadingUtils.getTempPath(job), "randomksubset-" + UUID.randomUUID().toString)
    val randSubsetTap = new Hfs(new SequenceFile((idOut, vectorOut)), randSubsetPath.toString, SinkMode.REPLACE)
    val randSubsetPipe = new Pipe("random k-subset")
      .using { new Random with Stateful }
      .map(() -> '__seed__) { (rand: Random, _: Unit) => rand.nextDouble() }
      .groupRandomly(NUM_MAPPERS) { _.sortBy('__seed__).take(k) }
      .discard('__seed__)
      .rename((idIn, vectorIn) -> (idOut, vectorOut))

    flowConnector.connect(sourceTap, tempTap, randSubsetPipe).complete()

    val iter = tempTap.openForRead(new HadoopFlowProcess(job))
    val collector = randSubsetTap.openForWrite(new HadoopFlowProcess(job))
    for (item <- iter.asScala.take(k)) collector.add(item)

    iter.close
    collector.close
    tempTap.delete(job)

    randSubsetTap
  }

  /**
    * Defines a partially applied function that calculates the minimum L2 norm between the given vector and each
    * vector in the cluster sequence.
    *
    * @tparam VecType type of incoming vector objects
    * @param clusterSeq Seq containing the clusters to calculate distances from
    * @param distFn Function that returns the distance between two vectors
    * @param vector Vector to calculate the minimum L2 norm from
    */
  def costFn[VecType](
    clusterSeq: Seq[VecType], distFn: (VecType, VecType) => Double)(vector: VecType): Double = {
    import scala.math.pow
    pow((clusterSeq map { c: VecType => distFn(c, vector) }).min, 2)
  }

  private def costFn[VecType](
    clusterSeq: Seq[(String, VecType)], distFn: (VecType, VecType) => Double)(vector: (String, VecType)): Double = costFn(clusterSeq.unzip._2, distFn)(vector._2)

  private def costFnKryoed[VecType](
    clusterSeqVessel: KryoVessel[Seq[Named[VecType]]], distFn: (VecType, VecType) => Double)(vector: Named[VecType]): Double = costFn(clusterSeqVessel.load.unzip._2, distFn)(vector._2)

  /**
    * Returns the sum of the seed costs for all vectors using the given sequence of clusters.
    *
    * @tparam VecType type of incoming vector objects
    * @param clusterSeq Seq containing the clusters to calculate distances from
    * @param sourceTap HadoopTap containing the full set of vectors
    * @param fieldsIn Tuple containing the id and vector fields for incoming vectors
    * @param fieldsOut Tuple containing the outgoing id and vector field names
    * @param distFn Function that returns the distance between two vectors
    * @param job JobConf containing the Path for output and the serialization settings
    * @return Double representing the sum of the individual seed costs for each vector
    */
  def seedCost[VecType](
    clusterSeq: Seq[Named[VecType]], sourceTap: HadoopTap,
    fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol), distFn: (VecType, VecType) => Double,
    job: JobConf = new JobConf): Double = {
    val (idIn, vectorIn) = fieldsIn
    val tempTap = new TmpHfs(job)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)

    val clusterSeqVessel = new KryoVessel(clusterSeq, job)
    def costForVector(vector: Named[VecType]): Double = costFnKryoed(clusterSeqVessel, distFn)(vector)

    def seedCostPipe = new Pipe("seed cost pipe")
      .mapTo((idIn, vectorIn) -> 'cost) { vector: Named[VecType] => costForVector(vector) }
      .groupRandomly(NUM_MAPPERS) { _.sum[Double]('cost) }
      .groupAll { _.sum[Double]('cost) }

    flowConnector.connect(sourceTap, tempTap, seedCostPipe).complete()

    val cost = ScaldingUtils.toSeq[Double](tempTap, job).head
    tempTap.delete(job)

    cost
  }

  /**
    * Returns the probability of sampling this vector, defined as the minimum L2 norm against the sequence
    * of clusters using the given distance function, times the over-sampling factor over the net cost.
    *
    * @tparam VecType type of incoming vector objects
    * @param clusterSeqVessel KryoVessel containing the Seq of clusters
    * @param seedCost Double representing the net cost over all vectors
    * @param l Double representing the over-sampling factor, with 2 x k as the default
    * @param distFn Function that returns the distance between two vectors
    * @param x vector to calculate the probability of sampling for
    * @return Double representing the probability that the given vector is sampled
    */
  def samplingFn[VecType](
    clusterSeqVessel: KryoVessel[Seq[Named[VecType]]],
    seedCost: Double, l: Double = 1.0, distFn: (VecType, VecType) => Double)(x: Named[VecType]
  ): Double = l * costFnKryoed(clusterSeqVessel, distFn)(x) / seedCost

  /**
    * Writes the ids of each vector along with the id of the closest cluster to the specified Tap.
    *
    * @tparam VecType type of incoming vector objects
    * @param fullSetTap HadoopTap containing the full set of vectors
    * @param vectorFields Tuple2 of Symbols representing the vector id and features
    * @param clusterSeq Seq of clusters to calculate squared errors from
    * @param distFn Function that returns the distance between two vectors
    * @param sinkTap HadoopTap containing vector-cluster id pairs
    * @param job JonConf containing the Path for output and the serialization settings
    */
  def categorizeVectors[VecType](
    fullSetTap: HadoopTap, vectorFields: (Symbol, Symbol) = ('vectorid, 'vectorFeatures), clusterSeq: Seq[Named[VecType]],
    distFn: (VecType, VecType) => Double, sinkTap: HadoopTap, job: JobConf = new JobConf) {
    val (vId, vFeatures) = vectorFields
    val clusterSeqVessel = new KryoVessel(clusterSeq, job)

    val assignmentPipe =
      new Pipe("cluster assignments")
        .project(vectorFields)
        .mapTo((vId, vFeatures) -> ('id, 'cluster)) {
          (vId: String, features: VecType) =>
            {
              val nearestCluster = clusterSeqVessel.load.foldLeft(("", Double.MaxValue)) {
                (nearest: (String, Double), next: Named[VecType]) =>
                  {
                    val dist = distFn(features, next._2)
                    if (dist < nearest._2) (next._1, dist) else nearest
                  }
              }._1
              (vId, nearestCluster)
            }
        }

    new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava).connect(fullSetTap, sinkTap, assignmentPipe).complete()
  }

  /**
    * Returns the mean squared error of all the vectors, using the given cluster sequence.
    *
    * @tparam VecType type of incoming vector objects
    * @param fullSetTap HadoopTap containing the full set of vectors
    * @param clusterSeq Seq of clusters to calculate squared errors from
    * @param vectorIn Symbol representing the Field containing the vector itself
    * @param distFn Function that returns the distance between two vectors
    * @param job JonConf containing the Path for output and the serialization settings
    * @return Double representing the mean squared error summed over all vectors
    */
  def meanSqError[VecType](
    fullSetTap: HadoopTap, clusterSeq: Seq[Named[VecType]], vectorIn: Symbol,
    distFn: (VecType, VecType) => Double, job: JobConf = new JobConf): Double = {
    import scala.math.pow

    val clusterSeqVessel = new KryoVessel(clusterSeq, job)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)

    val msePipe =
      new Pipe("mean squared error")
        .mapTo(vectorIn -> 'sqError) {
          features: VecType =>
            {
              val nearestCluster = clusterSeqVessel.load.foldLeft(("", Double.MaxValue)) {
                (nearest: (String, Double), next: Named[VecType]) =>
                  {
                    val dist = distFn(features, next._2)
                    if (dist < nearest._2) (next._1, dist) else nearest
                  }
              }
              pow(nearestCluster._2, 2)
            }
        }
        .groupAll { _.average('sqError -> 'meanSqError) }
        .project('meanSqError)
    val debugTempSink = new TmpHfs(job)

    flowConnector.connect(fullSetTap, debugTempSink, msePipe).complete()
    val error = ScaldingUtils.toSeq[Double](debugTempSink, job).head
    debugTempSink.delete(job)

    error
  }
}
