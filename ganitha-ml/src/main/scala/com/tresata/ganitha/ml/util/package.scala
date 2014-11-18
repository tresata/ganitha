package com.tresata.ganitha.ml.util

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

import cascading.flow.hadoop.{ HadoopFlowConnector, HadoopFlowProcess }
import cascading.pipe.Pipe
import cascading.tap.hadoop.Hfs
import cascading.tap.SinkMode
import cascading.tuple.{ Fields, Tuple, Tuples, TupleEntry => CTupleEntry }
import cascading.scheme.hadoop.{ SequenceFile => CSequenceFile }

import com.tresata.ganitha.util._

import com.twitter.scalding.Dsl._

/**
  * Provides common scalding classes and functions used for machine-learning algorithms.
  */
object `package` {
  case class StrDblMapVector(cardinality: Int, mapping: Map[String, Double])
  type Named[Vector] = (String, Vector)

  def toNamedSeq[VecType](vectorSeq: Seq[VecType]): Seq[Named[VecType]] =
    Range.inclusive(1, vectorSeq.size).view.map { _.toString }.zip(vectorSeq).force

  trait Stateful {
    def release() {}
  }

  /**
    * Returns the number of tuples in the given Tap.
    *
    * @param vectorTap HadoopTap containing tuples to count
    * @param job JobConf containing the Path for output and the serialization settings
    * @return Long representing the number of tuples in the Tap
    */
  def countTuples(vectorTap: HadoopTap, job: JobConf = new JobConf): Long = {
    val flowConnector = new HadoopFlowConnector(ScaldingUtils.toMap(job).asJava)
    val tempTap = new TmpHfs(job)
    val countPipe = new Pipe("count")
    val flow = flowConnector.connect(vectorTap, tempTap.asInstanceOf[HadoopTap], countPipe)
    flow.complete()
    flow.getFlowStats.getCounterValue("cascading.flow.SliceCounters", "Tuples_Written")
  }

  /**
    * Returns a HadoopTap where the id field of each vector is reset to 1 counting up to the number of tuples.
    *
    * Note: This method is only intended for re-indexing small sets of vectors, e.g. cluster centers, as it loads all
    * the vectors into a single TupleEntryIterator.
    *
    * @param fullSetTap HadoopTap containing the set of vectors to re-index
    * @param vectorFields Tuple of Symbols representing the id and vector Fields
    * @param job JobConf containing the Path for output and the serialization settings
    * @return HadoopTap containing the re-indexed vectors
    */
  def reIndexVectorTap(fullSetTap: HadoopTap, vectorFields: (Symbol, Symbol), job: JobConf = new JobConf): HadoopTap = {

    val uuid = UUID.randomUUID().toString
    val scratchPath = new Path(CascadingUtils.getTempPath(job), "reindexed-" + uuid)

    val iter = fullSetTap.openForRead(new HadoopFlowProcess(job))
    val reIndexedTap = new Hfs(new CSequenceFile(vectorFields), scratchPath.toString, SinkMode.REPLACE)
    val collector = reIndexedTap.openForWrite(new HadoopFlowProcess(job))

    for (item <- iter.asScala.zipWithIndex)
      collector.add(new Tuple((item._2 + 1).toString, item._1.getTuple.getObject(1)))

    iter.close
    collector.close

    reIndexedTap.asInstanceOf[HadoopTap]
  }

  /**
    * Returns a Pipe that converts the first Field into an id, and converts the other fields into a StrDblMapVector,
    * using the field names for keys. This method expects a header to establish the keys for the Map.
    *
    * @param dataPipe Pipe containing the fields and values to convert into String, Double Maps
    */
  def vectorizeFields(dataPipe: Pipe): Pipe = {
    dataPipe
      .mapTo('* -> ('id, 'vector)) { x: CTupleEntry =>
        {
          val id = x.getTuple.getString(0)
          val keys = x.getFields.iterator.asScala.drop(1)
          val values = for (item <- x.getTuple.iterator.asScala.drop(1)) yield Tuples.toDouble(item)
          val vectorMap = Map() ++ (for ((key, value) <- keys zip values) yield (key.toString, value))
          (id, new StrDblMapVector(vectorMap.size, vectorMap))
        }
      }
  }

  /**
    * Returns a Pipe that converts the first Field into an id, and converts the other fields into a StrDblMapVector,
    * using the field names for keys. This method expects a header to establish the keys for the Map.
    *
    * @param dataPipe Pipe containing the fields and values to convert into String, Double Maps
    * @param idField Fields containing the id field for the vector
    * @param vectorFields Fields containing the coordinates of the vector
    */
  def vectorizeFields(dataPipe: Pipe, idField: Fields, vectorFields: Fields): Pipe = {
    require(idField.size == 1, "Only specify one field for the vector id")
    dataPipe
      .project(idField.append(vectorFields))
      .thenDo((pipe: Pipe) => vectorizeFields(pipe))
  }

  /**
    * Returns one choice from a probabilistic distribution of vectors.
    *
    * @tparam T type of incoming vector identifiers (typically an Int from 1 to n)
    * @param seqWithProbs Seq of Tuples containing the vectors and their sampling probabilities
    * @return identifier of type T that was chosen from the distribution
    */
  def choose1FromDistribution[T](seqWithProbs: Seq[(T, Double)]): T =
    choose1FromDistribution(seqWithProbs.unzip._2.sum)(seqWithProbs)

  /**
    * Returns one choice from a probabilistic distribution of vectors.
    *
    * @tparam T type of incoming vector identifiers (typically an Int from 1 to n)
    * @param netProbability Double representing the net probability
    * @param seqWithProbs Seq of Tuples containing the vectors and their sampling probabilities
    * @return identifier of type T that was chosen from the distribution
    */
  def choose1FromDistribution[T](netProbability: Double)(seqWithProbs: Seq[(T, Double)]): T = {
    import scala.util.Random
    import scala.util.control.Breaks._
    require(seqWithProbs.size > 0)

    val rand = new Random
    val seed = rand.nextDouble() * netProbability
    var cumulativeProb = 0.0
    var choice: T = seqWithProbs(0)._1

    breakable {
      for ((item, prob) <- seqWithProbs) {
        cumulativeProb += prob
        if (seed < cumulativeProb) { choice = item; break }
      }
    }

    choice
  }
}
