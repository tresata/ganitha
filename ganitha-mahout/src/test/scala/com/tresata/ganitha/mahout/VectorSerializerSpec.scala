package com.tresata.ganitha.mahout

import scala.collection.JavaConverters._

import org.apache.mahout.math.Vector

import cascading.flow.hadoop.HadoopFlowConnector
import cascading.pipe.Pipe
import cascading.scheme.hadoop.{ TextDelimited, SequenceFile }
import cascading.tap.SinkMode
import cascading.tap.hadoop.Hfs

import com.twitter.scalding.Dsl._

import org.scalatest.FunSpec

import com.tresata.ganitha.util._
import Implicits._

class VectorSerializerSpec extends FunSpec {

  describe("A Vector") {

    it("should be writable to and readable from cascading sequence files") {
      val props = VectorSerializer.register(ScaldingUtils.withScaldingDefaults(Map[AnyRef, AnyRef]()))

      val source1 = new Hfs(new TextDelimited(('a, 'b, 'c, 'd), "|"), "test/data/vectors.txt")
      val sink1 = new Hfs(new SequenceFile('vector), "tmp/vectors", SinkMode.REPLACE)
      val pipe1 = new Pipe("test")
      .mapTo(('a, 'b, 'c, 'd) -> 'vector){ x : (Double, Double, Double, Double) => RichVector(x.productIterator.asInstanceOf[Iterator[Double]].toArray) }
      .debug
      new HadoopFlowConnector(props.asJava).connect(source1.asInstanceOf[HadoopTap], sink1.asInstanceOf[HadoopTap], pipe1).complete()

      val source2 = sink1
      val sink2 = new Hfs(new TextDelimited(('a, 'b, 'c, 'd), "|"), "tmp/vectors.txt", SinkMode.REPLACE)
      val pipe2 = new Pipe("test")
      .debug
      .mapTo('vector -> ('a, 'b, 'c, 'd)){ x : Vector => (x(0), x(1), x(2), x(3)) }
      new HadoopFlowConnector(props.asJava).connect(source2.asInstanceOf[HadoopTap], sink2.asInstanceOf[HadoopTap], pipe2).complete()

      val seq = ScaldingUtils.toSeq[Vector](new Hfs(new SequenceFile('a), "tmp/vectors").asInstanceOf[HadoopTap], ScaldingUtils.toJobConf(props))
      assert(seq(0) sameElements List(1, 2, 3, 4))
      assert(seq(1) sameElements List(5, 6, 7, 8))
      assert(seq(2) sameElements List(9, 10, 11, 12))
    }

  }

}
