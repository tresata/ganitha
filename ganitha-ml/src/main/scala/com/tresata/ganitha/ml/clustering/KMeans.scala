package com.tresata.ganitha.ml.clustering

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.util.Random

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapred.JobConf

import cascading.flow.hadoop.HadoopFlowConnector
import cascading.operation.filter.Sample
import cascading.pipe.Pipe
import cascading.scheme.hadoop.SequenceFile
import cascading.tap.hadoop.Hfs
import cascading.tap.SinkMode

import com.twitter.scalding.Dsl._

import com.tresata.ganitha.util._
import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.ml.util.VectorImplicits._
import com.tresata.ganitha.mahout.Implicits._
import com.tresata.ganitha.mahout.VectorSerializer
import com.tresata.ganitha.util.FunctionImplicits._ // change to com.twitter.scalding.FunctionImplicits

import org.slf4j.LoggerFactory

/**
  * Partitions observations into k clusters in which each observation belongs to the cluster with the nearest mean,
  * in order to minimize the mean squared error.
  *
  * This class provides an implementation of Lloyd's algorithm to iteratively refine k clusters given an initial seed
  * set of clusters, as well seed initilization algorithms including K-Means++, and the more scalable K-Means||.
  *
  * Data points are passed in through a Tap containing an ID field and a single vector field. The use of this class is
  * not justified for small datasets (< 100MB) due to the heavy overhead; a single-machine implementation would be more
  * appropriate, particularly for larger values of k.
  *
  * @constructor Creates a new KMeans object using the specified distance function and Vector type
  * @tparam VecType type of objects that incoming vectors are to be de-serialized into, which should be accompanied by
  *         a VectorHelper object that defines the necessary vector operations
  * @param distFn Function that takes two vectors and returns a Double value representing their distance
  */
class KMeans[VecType <: AnyRef](distFn: (VecType, VecType) => Double)(implicit helper: VectorHelper[VecType]) extends Serializable {

  import KMeans._
  type NamedVector = Named[VecType]

  /**
    * Runs Lloyd's algorithm to refine clusters given an initial seed set in order to minimize the net squared error of
    * points to their nearest cluster centroid.
    *
    * @param vectorTap HadoopTap containing the full set of vectors to partition into clusters
    * @param vectorFields Symbols representing the ID and vector Fields
    * @param clusterTap HadoopTap containing the initial seed set of clusters, with the number of vectors determining 'k'
    * @param clusterFields Symbols representing the ID and vector Fields for the initial cluster set
    * @param sinkTap HadoopTap to emit the final clusters to
    * @param jobConf JobConf containing configurations, including custom settings for the maximum number of Lloyd
    *        iterations and a distance threshold for determining convergence
    */
  def runAlgo(
    vectorTap: HadoopTap, clusterTap: HadoopTap,
    sinkTap: HadoopTap, clusterSinkTap: HadoopTap,
    vectorFields: (Symbol, Symbol) = ('vectorid, 'vectorFeatures),
    clusterFields: (Symbol, Symbol) = ('clusterid, 'clusterFeatures),
    jobConf: JobConf = new JobConf) {
    val job = new JobConf(jobConf)
    ScaldingUtils.setScaldingDefaults(job)
    VectorSerializer.register(job)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)

    val convergenceDelta: Double = job.getFloat("convergenceDelta", 0.001f).toDouble
    val maxIter: Int = job.getInt("maxIter", 20)
    require(maxIter > 0)

    val scratchPath = new Path(CascadingUtils.getTempPath(job), "kmeans-" + UUID.randomUUID().toString)
    CascadingUtils.setTempPath(job, scratchPath)

    // initial clusters
    flowConnector.connect(clusterTap, getClusterStepTap(iter = 0, fields = clusterFields, job), new Pipe("initial clusters")).complete()

    // Lloyd iterations
    var i = 1
    var changes: (Int, Int) = (Int.MaxValue, 0)
    do {
      val clusterSeq: Seq[NamedVector] = getClusterStepSeq(iter = (i - 1), fields = clusterFields, job)
      val k: Int = clusterSeq.size

      logger.info("kmeans iteration            {}", i)
      logger.debug("kmeans number of clusters:  {}", clusterSeq.size)

      val pipe = getStepPipe(i, clusterSeq, fieldsIn = vectorFields, fieldsOut = clusterFields, job)
      val sink = getClusterStepTap(iter = i, fields = clusterFields, job)
      flowConnector.connect(vectorTap, sink, pipe).complete()

      val nextClusterSeq = getClusterStepSeq(iter = i, fields = clusterFields, job)

      changes = clusterChanges(clusterSeq, nextClusterSeq, distFn, convergenceDelta)

      logger.info("kmeans clusters changed:    {}", changes._1)
      logger.info("kmeans clusters unchanged:  {}", changes._2)
      logger.debug("Previous cluster centers:")
      for ((id, vector) <- clusterSeq) { logger.debug("{}: {}", id, helper.toString(vector)) }

      logger.info("Cluster centers:")
      for ((id, vector) <- nextClusterSeq) { logger.info("{}: {}", id, helper.toString(vector)) }

      i += 1
    } while (changes._1 > 0 && i <= maxIter)

    // final cluster set
    logger.info("outputting cluster vectors to sink")
    val finalClustersTap = getClusterStepTap(iter = (i - 1), fields = clusterFields, job)
    flowConnector.connect(finalClustersTap, clusterSinkTap, new Pipe("final clusters")).complete()

    // assign cluster IDs to vectors
    logger.info("outputting vector assignments to sink")
    val finalClustersSeq: Seq[NamedVector] = ScaldingUtils.toSeq[NamedVector](finalClustersTap, job)
    categorizeVectors(vectorTap, vectorFields, finalClustersSeq, distFn, sinkTap, job)

    // calculate mean squared error (debug mode only)
    if (logger.isDebugEnabled) {
      val error = meanSqError(fullSetTap = vectorTap, finalClustersSeq, vectorIn = vectorFields._2, distFn, job)
      logger.debug("Mean Squared Error over all vectors: {}", error)
    }

    // clean up temp path
    logger.info("deleting " + scratchPath)
    FileSystem.get(jobConf).delete(scratchPath, true)
  }

  private def getClusterStepTap(iter: Int, fields: (Symbol, Symbol), job: JobConf): HadoopTap = {
    val stepPath = new Path(CascadingUtils.getTempPath(job), "kmeans-step-%02d".format(iter))
    new Hfs(new SequenceFile(fields), stepPath.toString, SinkMode.REPLACE)
  }

  private def getClusterStepSeq(iter: Int, fields: (Symbol, Symbol), job: JobConf): Seq[NamedVector] = {
    require(iter >= 0)
    ScaldingUtils.toSeq[NamedVector](getClusterStepTap(iter, fields, job), job)
  }

  private def getStepPipe(i: Int, prevClusterSeq: Seq[NamedVector], fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol), job: JobConf): Pipe = {
    val (idIn, vectorIn) = fieldsIn
    val (idOut, vectorOut) = fieldsOut
    val prevClusterSeqVessel = new KryoVessel(prevClusterSeq, job)

    // Expectation/Assignment Step

    val neighborhoodPipe = new Pipe("nearest clusters: iteration " + i)
      .map(fieldsIn -> idOut) {
        features: NamedVector => {
          val nearestCluster = prevClusterSeqVessel.load.foldLeft(("", Double.MaxValue)) {
            (nearest: (String, Double), next: NamedVector) => {
              val dist = distFn(features._2, next._2)
              if (dist < nearest._2) (next._1, dist) else nearest
            }
          }
          nearestCluster._1
        }
      }

    // Maximization/Update step

    val centroidsPipe = new Pipe("new centroids", neighborhoodPipe)
      .groupBy(idOut) {
        _.size('count)
          .foldLeft(vectorIn -> vectorOut)(null.asInstanceOf[VecType]) {
            (newCenter: VecType, next: VecType) => {
              if (newCenter == null) next
              else helper.plus(newCenter, next)
            }
          }
      }
      .map((idOut, vectorOut, 'count) -> fieldsOut) {
        (id: String, vector: VecType, count: Int) => {
          logger.debug("Cluster {} has size {}", id, count)
          (id, helper.divBy(vector, count))
        }
      }

    centroidsPipe
  }

  /**
    * Returns a HadoopTap containing the result of the specified seeding algorithm's initial cluster centers, for use
    * in the first iteration of the K-Means algorithm.
    *
    * @param vectorTap HadoopTap containing the full set of vectors to partition into clusters
    * @param k Int representing the number of clusters to generate
    * @param seedAlgo String representing the algorithm to use for seeding--if none is specified, the optimal method is
    *        determined. Possible values include '++', '||', and 'random'.
    * @param fieldsIn Symbols representing the ID and vector Fields for the incoming vectorTap
    * @param fieldsOut Symbols representing the ID and vector Fields to emit through the outgoing HadoopTap
    * @param jobConf JobConf containing configurations, including custom settings for the oversampling factor and
    *        maximum number of iterations (K-Means||), and a size estimate of the vectorTap for optimization purposes
    * @return HadoopTap containing the id-vector pairs of the resulting seed set
    */
  def initialClustersTap(
    vectorTap: HadoopTap, k: Int, seedAlgo: String = "optimal",
    fieldsIn: (Symbol, Symbol) = ('vectorid, 'vectorFeatures),
    fieldsOut: (Symbol, Symbol) = ('clusterid, 'clusterFeatures),
    jobConf: JobConf = new JobConf): HadoopTap = {
    val job = new JobConf(jobConf)
    ScaldingUtils.setScaldingDefaults(job)
    VectorSerializer.register(job)
    val scratchPath = new Path(CascadingUtils.getTempPath(job), "kmeansseeding-" + UUID.randomUUID().toString)
    CascadingUtils.setTempPath(job, scratchPath)
    val numVectors =
      if (job.get("vectorTapSizeEstimate") != null) job.get("vectorTapSizeEstimate").toLong
      else countTuples(vectorTap, job)

    val seedTap = seedAlgo match {
      case "||" => initialClustersTapParallel(vectorTap, k, numVectors, fieldsIn, fieldsOut, job)
      case "++" => initialClustersTapIterative(vectorTap, k, numVectors, fieldsIn, fieldsOut, job)
      case "random" => initialClustersTapRandom(vectorTap, k, numVectors, fieldsIn, fieldsOut, job)
      case "optimal" => initialClustersTapOptimal(vectorTap, k, numVectors, fieldsIn, fieldsOut, job)
    }

    reIndexVectorTap(seedTap, vectorFields = fieldsOut, job)
  }

  private def initialClustersTapOptimal(
    fullSetTap: HadoopTap, k: Int, numVectors: Long,
    fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol),
    job: JobConf): HadoopTap = {
    if (k < 5 || (k < 20 && numVectors < 10000L))
      initialClustersTapIterative(fullSetTap, k, numVectors, fieldsIn, fieldsOut, job)
    else
      initialClustersTapParallel(fullSetTap, k, numVectors, fieldsIn, fieldsOut, job)
  }

  private def initialClustersTapRandom(
    fullSetTap: HadoopTap, k: Int, numVectors: Long,
    fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol),
    job: JobConf): HadoopTap = {
    val scratchPath = new Path(CascadingUtils.getTempPath(job), "kmeansRandomSeeds-" + UUID.randomUUID().toString)
    CascadingUtils.setTempPath(job, scratchPath)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)

    logger.info("Initializing K-Means algorithm with {} random clusters from {} vectors", k, numVectors)
    val subsetTap = randomKSubset(sourceTap = fullSetTap, k, fieldsIn, fieldsOut, job)
    reIndexVectorTap(subsetTap, vectorFields = fieldsOut, job)
  }

  private def initialClustersTapIterative(
    fullSetTap: HadoopTap, k: Int, numVectors: Long,
    fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol),
    jobConf: JobConf): HadoopTap = {
    val sampleSplitFactor: Double = numVectors.toDouble / IDS_PER_MAPPER

    val job = new JobConf(jobConf)
    val scratchPath = new Path(CascadingUtils.getTempPath(job), "kmeans++Seeds-" + UUID.randomUUID().toString)
    CascadingUtils.setTempPath(job, scratchPath)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)

    val (idIn, vectorIn) = fieldsIn
    val (idOut, vectorOut) = fieldsOut

    // K-Means++
    logger.info("Running K-Means++ algorithm on {} vectors with k = {}", numVectors, k)

    def nextCluster(clusterSeq: Seq[NamedVector], seedCost: Double): NamedVector = {
      val clusterSeqVessel = new KryoVessel(clusterSeq, job)
      def samplingProb(vector: NamedVector) = samplingFn(clusterSeqVessel, seedCost, l = 1.0, distFn)(vector)
      val clusterIDSeq: IndexedSeq[String] = clusterSeq.unzip._1.toIndexedSeq
      logger.debug("current cluster sequence: {}", clusterIDSeq)

      val tempTap1 = new TmpHfs(job)
      val samplingPipe = new Pipe("k-means++ sampling pipe")
        .then(if (sampleSplitFactor > 1.15) (pipe: Pipe) => pipe.sample(1 / sampleSplitFactor) else identity[Pipe])
        .filter(idIn) { id: String => !clusterIDSeq.contains(id) }
        .using { new Random with Stateful }
        .map(() -> 'seed) { (rand: Random, _: Unit) => rand.nextDouble() }
        .map((idIn, vectorIn) -> 'prob) { vector: NamedVector => samplingProb(vector) }
        .groupAll {
          _.mapList((idIn, 'prob) -> idIn) {
            (list: List[(String, Double)]) => choose1FromDistribution(list)
          }
        }
      flowConnector.connect(fullSetTap, tempTap1, samplingPipe).complete()
      val nextClusterID: String = ScaldingUtils.toSeq[String](tempTap1, job).head
      tempTap1.delete(job)

      val tempTap2 = new TmpHfs(job)
      val choicePipe = new Pipe("k-means++ cluster selection pipe")
        .filter(idIn) { id: String => id == nextClusterID }
        .rename(fieldsIn -> fieldsOut)
        .project(idOut, vectorOut)
      flowConnector.connect(fullSetTap, tempTap2, choicePipe).complete()
      val nextCluster = ScaldingUtils.toSeq[NamedVector](tempTap2, job).head
      tempTap2.delete(job)

      nextCluster
    }

    val firstSeedTap = randomKSubset(sourceTap = fullSetTap, 1, fieldsIn, fieldsOut, job)
    val clusterBuffer: Buffer[NamedVector] = ScaldingUtils.toSeq[NamedVector](firstSeedTap, job).toBuffer
    var clusterCost: Double = seedCost(clusterBuffer, fullSetTap, fieldsIn, fieldsOut, distFn, job)

    logger.debug("k-means++ initial seed vector is {}", helper.toString(clusterBuffer(0)._2))
    logger.debug("initial cluster cost is {}", clusterCost)
    logger.debug("performing {} iterations of k-means++", (k - 1))

    for (i <- 1 until k) {
      val addedCluster: NamedVector = nextCluster(clusterBuffer, clusterCost)
      clusterBuffer += addedCluster
      clusterCost = seedCost(clusterBuffer, fullSetTap, fieldsIn, fieldsOut, distFn, job)

      logger.debug("k-means++ iteration {}/{}", i, (k - 1))
      logger.debug("cluster chosen : {} = {}", addedCluster._1, helper.toString(addedCluster._2))
      logger.debug("cluster seed list now has size {}", clusterBuffer.size)
      logger.debug("cluster cost is now {}", clusterCost)
    }

    logger.info("k-means++ sampling stage complete.")
    logger.debug("cluster cost is {}", clusterCost)
    logger.debug("cluster IDs:")
    for (v <- clusterBuffer.zipWithIndex) logger.debug("{} : {}", v._2, (v._1)._1)

    val clusterSeedTap = ScaldingUtils.toTempTap[NamedVector](fields = fieldsOut, clusterBuffer, job)
    val initialClusters = randomKSubset(sourceTap = clusterSeedTap, k, fieldsIn = fieldsOut, fieldsOut, job)

    clusterSeedTap.delete(job)

    initialClusters
  }

  private def initialClustersTapParallel(
    fullSetTap: HadoopTap, k: Int, numVectors: Long,
    fieldsIn: (Symbol, Symbol), fieldsOut: (Symbol, Symbol),
    job: JobConf): HadoopTap = {
    val scratchPath = new Path(CascadingUtils.getTempPath(job), "kmeans||Seeds-" + UUID.randomUUID().toString)
    CascadingUtils.setTempPath(job, scratchPath)
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)
    val oversamplingFactor = job.getFloat("kmeans||oversamplingFactor", 2.0f * k).toDouble
    val maxRounds = job.getInt("kmeans||maxRounds", 5)

    val (idIn, vectorIn) = fieldsIn
    val (idOut, vectorOut) = fieldsOut

    // K-Means||
    logger.info("Running K-Means|| algorithm on {} vectors with k = {}, l = {}",
      Array[AnyRef](numVectors.toString, k.toString, oversamplingFactor.toString))

    def newClusters(clusterSeq: Seq[NamedVector], seedCost: Double): Seq[NamedVector] = {
      val clusterSeqVessel = new KryoVessel(clusterSeq, job)
      def samplingProb(vector: NamedVector): Double = samplingFn(clusterSeqVessel, seedCost, l = oversamplingFactor, distFn)(vector)

      val tempTap = new TmpHfs(job)
      val samplingPipe = new Pipe("k-means|| sampling pipe")
        .using { new Random with Stateful }
        .map(() -> 'seed) { (rand: Random, _: Unit) => rand.nextDouble() }
        .map(fieldsIn -> 'prob) { vector: NamedVector => samplingProb(vector) }
        .filter('seed, 'prob) { (seed: Double, prob: Double) => (seed < prob) }
        .discard('seed, 'prob)
        .rename(fieldsIn -> fieldsOut)
      flowConnector.connect(fullSetTap, tempTap, samplingPipe).complete()
      val newClusters = ScaldingUtils.toSeq[NamedVector](tempTap, job)
      tempTap.delete(job)

      newClusters
    }

    def kMeansWeighted(weightedVectorSeq: Seq[(VecType, Double)]): Seq[VecType] = {
      logger.info("Running K-Means++ algorithm on {} vectors with k = {}", weightedVectorSeq.size, k)

      var vectorSeqWithProbs: IndexedSeq[(VecType, Double)] = weightedVectorSeq.toIndexedSeq
      val vectors: IndexedSeq[VecType] = vectorSeqWithProbs.unzip._1
      val vectorIDSeq: IndexedSeq[Int] = Range(0, vectors.size)

      val firstClusterID: Int = choose1FromDistribution(vectorIDSeq.zip(vectorSeqWithProbs.map { _._2 }))
      var clusterIDSeq: IndexedSeq[Int] = IndexedSeq(firstClusterID)
      var clusterSeq: Seq[VecType] = Seq(vectors(firstClusterID))
      logger.debug("k-means++ cluster 1/{}: {}", k, firstClusterID)

      for (i <- 2 to k) {
        // re-adjust probability weights
        vectorSeqWithProbs = vectorSeqWithProbs.map { vecWithProb =>
          if (clusterIDSeq.contains(vecWithProb._1)) (vecWithProb._1, 0.0)
          else (vecWithProb._1, costFn(clusterSeq, distFn)(vecWithProb._1))
        }

        // choose next cluster and add to list
        val nextClusterID = choose1FromDistribution(vectorIDSeq.zip(vectorSeqWithProbs.map { _._2 }))
        clusterIDSeq = clusterIDSeq :+ nextClusterID
        clusterSeq = clusterSeq :+ vectors(nextClusterID)
        logger.debug("k-means++ cluster {}/{}: {}", Array[AnyRef](i.toString, k.toString, nextClusterID.toString))
      }

      clusterSeq
    }

    val firstSeedTap = randomKSubset(sourceTap = fullSetTap, 1, fieldsIn, fieldsOut, job)
    val clusterBuffer: Buffer[NamedVector] = ScaldingUtils.toSeq[NamedVector](firstSeedTap, job).toBuffer
    var clusterCost: Double = seedCost(clusterBuffer, fullSetTap, fieldsIn, fieldsOut, distFn, job)
    val numIterations: Int = scala.math.ceil(scala.math.log(clusterCost)).toInt

    logger.debug("k-means|| initial seed vector is {}", helper.toString(clusterBuffer(0)._2))
    logger.debug("initial cluster cost is {}", clusterCost)
    logger.debug("performing up to {} (and as few as {}) iterations of k-means||", numIterations, maxRounds)

    for (i <- 1 until numIterations if (i <= maxRounds || clusterBuffer.size < k)) {
      val addedClusters = newClusters(clusterBuffer, clusterCost)
      clusterBuffer ++= addedClusters
      clusterCost = seedCost(clusterBuffer, fullSetTap, fieldsIn, fieldsOut, distFn, job)

      logger.debug("k-means|| iteration {}/{}", i, numIterations)
      logger.debug("adding {} cluster(s) to seed list", addedClusters.size)
      logger.debug("cluster seed list now has size {}", clusterBuffer.size)
      logger.debug("cluster cost is now {}", clusterCost)
    }

    logger.info("k-means|| sampling stage complete.")
    logger.debug("cluster cost is {}", clusterCost)

    val clusterBufferVessel = new KryoVessel(clusterBuffer, job)
    val clusterWeightsTap = new TmpHfs(job)
    val clusterWeightsPipe = new Pipe("seed cluster weights")
      .mapTo(fieldsIn -> 'closestCluster) {
        (feature: String, vector: VecType) => {
          val nearestCluster = clusterBufferVessel.load.foldLeft(("", Double.MaxValue)) {
            (nearest: (String, Double), next: NamedVector) => {
              val dist = distFn(vector, next._2)
              if (dist < nearest._2) (next._1, dist) else nearest
            }
          }
          nearestCluster._1
        }
      }
      .groupBy('closestCluster) {
        _.size('weights)
      }
    flowConnector.connect(fullSetTap, clusterWeightsTap, clusterWeightsPipe).complete()

    val idWeightSeq: Seq[(String, Double)] = ScaldingUtils.toSeq[(String, Double)](clusterWeightsTap, job)
    val idWeightMap: Map[String, Double] = idWeightSeq.toMap
    val weightedClustersSeq: Seq[(VecType, Double)] = clusterBuffer.map { v => (v._2, idWeightMap.getOrElse(v._1, 0.0)) }

    clusterWeightsTap.delete(job)

    // use weighted k-means++ to obtain the final initial cluster set
    logger.info("Running weighted k-means++")
    logger.debug("cluster IDs (weights):")
    for (v <- idWeightSeq.zipWithIndex) logger.debug("{} : {} ({})",
      Array[AnyRef](v._2.toString, (v._1)._1.toString, (v._1)._2.toString))
    val initialClustersSeq: Seq[NamedVector] = toNamedSeq(kMeansWeighted(weightedClustersSeq))
    logger.info("weighted k-means++ complete.")

    ScaldingUtils.toTempTap[NamedVector](fields = fieldsIn, initialClustersSeq, job)
  }

}

object KMeans {
  private val logger = LoggerFactory.getLogger(KMeans.getClass)
}
