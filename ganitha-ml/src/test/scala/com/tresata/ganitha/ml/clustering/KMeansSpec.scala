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
      // first vectorize the input and write to a temporary sequence file
      com.twitter.scalding.Tool.main("com.tresata.ganitha.ml.util.VectorizeJob --hdfs --input %s --id id --features x y --vectors %s"
        .format("test/data/fourgaussians/vectors.bsv", "tmp/fourgaussians_seqfile").split("\\s+"))

      val dataPointsTap = new Hfs(new SequenceFile(('vectorid, 'vectorFeatures)), "tmp/fourgaussians_seqfile").asInstanceOf[HadoopTap]
      val assignmentTap = new Hfs(new TextDelimited(), "tmp/fourgaussians/vector_assignments.bsv", SinkMode.REPLACE).asInstanceOf[HadoopTap]
      val centroidsTap = new Hfs(new TextDelimited(), "tmp/fourgaussians/clusters.bsv", SinkMode.REPLACE).asInstanceOf[HadoopTap]
      val kmeans = new KMeans(distFn = StrDblMapVectorHelper.euclidean)
      val initClustersTap : HadoopTap = kmeans.initialClustersTap(dataPointsTap, k = 4)
      kmeans.runAlgo(vectorTap = dataPointsTap, clusterTap = initClustersTap, sinkTap = assignmentTap, clusterSinkTap = centroidsTap)
    }

  }

}
