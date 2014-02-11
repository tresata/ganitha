package com.tresata.ganitha.ml.clustering

import com.twitter.scalding.{ Job, Args, Mode, Source, AccessMode, Read, Write, HadoopMode, SequenceFile, Csv }

import com.tresata.ganitha.util._
import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.ml.util.VectorImplicits._

class KMeansJob(args: Args) extends VectorizeJob(args) {
  override def next: Option[Job] = Some(new KMeansSeqFileJob(args))
}

class KMeansSeqFileJob(args: Args) extends Job(args) {
  override def validate {}
  override def run: Boolean = {
    require(mode.isInstanceOf[HadoopMode], "this job does not support local mode")
    def createTap(source: Source, readOrWrite: AccessMode): HadoopTap = source.createTap(readOrWrite)(mode).asInstanceOf[HadoopTap]

    val vectorTap = createTap(SequenceFile(args("vectors"), ('vectorid, 'vectorFeatures)), Read)
    val vectorSinkTap = createTap(Csv(args("vectorOutput")), Write)
    val clusterSinkTap = createTap(SequenceFile(args("clusterOutput"), ('clusterid, 'clusterFeatures)), Write)

    val k = args("k").toInt
    val seedAlgo = args.getOrElse("seedAlgo", "optimal") // ["||", "++", "random", "optimal"]
    val kmeans = (args.getOrElse("vecType", "StrDblMapVector"), args.getOrElse("distFn", "euclidean")) match {
      case ("StrDblMapVector", "cosine") => new KMeans(StrDblMapVectorHelper.cosine _)
      case ("StrDblMapVector", "euclidean") => new KMeans(StrDblMapVectorHelper.euclidean _)
      case ("MahoutVector", "cosine") => new KMeans(MahoutVectorHelper.cosine _)
      case ("MahoutVector", "euclidean") => new KMeans(MahoutVectorHelper.euclidean _)
      case _ => new KMeans(StrDblMapVectorHelper.euclidean _)
    }

    // form initial cluster assignments
    val clusterTap = kmeans.initialClustersTap(vectorTap, k, seedAlgo)

    // run k-means clustering
    kmeans.runAlgo(vectorTap, clusterTap, vectorSinkTap, clusterSinkTap)

    true
  }
}
