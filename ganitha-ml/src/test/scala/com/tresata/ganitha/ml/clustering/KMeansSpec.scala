package com.tresata.ganitha.ml.clustering

import cascading.tap.SinkMode
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.{ TextDelimited, SequenceFile }

import com.twitter.scalding.Dsl._

import com.tresata.ganitha.util.HadoopTap
import com.tresata.ganitha.ml.util._
import com.tresata.ganitha.ml.util.VectorImplicits._

import org.scalatest.FunSpec

class KMeansSpec extends FunSpec {

  describe("A KMeans object") {

    it("should classify a set of four gaussian distributions") {
      val dataPointsTap = new Hfs(new SequenceFile(('vectorid, 'vectorFeatures)), "test/data/fourgaussians_seqfile").asInstanceOf[HadoopTap]
      val assignmentTap = new Hfs(new TextDelimited(), "tmp/fourgaussians/vector_assignments.bsv", SinkMode.REPLACE).asInstanceOf[HadoopTap]
      val centroidsTap = new Hfs(new TextDelimited(), "tmp/fourgaussians/clusters.bsv", SinkMode.REPLACE).asInstanceOf[HadoopTap]
      val kmeans = new KMeans(distFn = StrDblMapVectorHelper.euclidean)
      val initClustersTap : HadoopTap = kmeans.initialClustersTap(dataPointsTap, k = 4)
      kmeans.runAlgo(vectorTap = dataPointsTap, clusterTap = initClustersTap, sinkTap = assignmentTap, clusterSinkTap = centroidsTap)
    }

  }

}
