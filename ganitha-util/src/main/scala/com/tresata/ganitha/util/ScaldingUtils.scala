package com.tresata.ganitha.util

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet

import cascading.flow.hadoop.HadoopFlowProcess
import cascading.tuple.{ Fields, Tuple => CTuple }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import com.tresata.ganitha.util._

import com.twitter.scalding.{ TupleConverter, TupleSetter }

/**
  * Utility class for scalding.
  */
object ScaldingUtils {
  def addToSeparatedStrings(value: Option[String], sep: String, updates: String*): String = {
    val set = LinkedHashSet[String]()
    value.map(_.split(sep).foreach(set += _))
    updates.foreach(set += _)
    set.mkString(sep)
  }

  def addToSeparatedStrings(map: Map[AnyRef, AnyRef], key: String, sep: String, updates: String*): Map[AnyRef, AnyRef] =
    map + (key -> addToSeparatedStrings(map.get(key).map(_.toString), sep, updates: _*))

  def addToStrings(value: Option[String], updates: String*): String = addToSeparatedStrings(value, ",", updates: _*)

  def addToStrings(map: Map[AnyRef, AnyRef], key: String, updates: String*): Map[AnyRef, AnyRef] =
    map + (key -> addToStrings(map.get(key).map(_.toString), updates: _*))

  private val ioSerVals = List[String]("org.apache.hadoop.io.serializer.WritableSerialization",
    "cascading.tuple.hadoop.TupleSerialization",
    "com.twitter.scalding.serialization.KryoHadoop")

  /**
    * Returns a new config map with the proper defaults set for scalding to function.
    * Should be used with HadoopFlowConnector for any job that uses scalding or else it might fail.
    * Note that it is not necessary to set these defaut if the job is launched using scalding's Job class.
    */
  def withScaldingDefaults(map: Map[AnyRef, AnyRef]): Map[AnyRef, AnyRef] = {
    map +
      ("io.serializations" -> addToStrings(map.get("io.serializations").map(_.toString), ioSerVals: _*)) +
      ("cascading.flow.tuple.element.comparator" -> "com.twitter.scalding.IntegralComparator") +
      ("cascading.spill.threshold" -> map.getOrElse("cascading.spill.threshold", "100000")) +
      ("cascading.spill.map.threshold" -> map.getOrElse("cascading.spill.map.threshold", "100000")) +
      ("mapreduce.task.classpath.user.precedence" -> "true") +
      ("mapreduce.user.classpath.first" -> "true")
  }

  /**
    * Same as withScaldingDefaults except for mutable Configuration objects that get modified in place.
    */
  def setScaldingDefaults(conf: Configuration) {
    conf.set("io.serializations", addToStrings(Option[String](conf.get("io.serializations")), ioSerVals: _*))
    conf.set("cascading.flow.tuple.element.comparator", "com.twitter.scalding.IntegralComparator")
    conf.set("cascading.spill.threshold", conf.get("cascading.spill.threshold", "100000"))
    conf.set("cascading.spill.map.threshold", conf.get("cascading.spill.map.threshold", "100000"))
    conf.set("mapreduce.task.classpath.user.precedence", "true")
    conf.set("mapreduce.user.classpath.first", "true")
  }

  /**
    * Converts hadoop configuration to map
    */
  def toMap(conf: Configuration): Map[AnyRef, AnyRef] = conf.asScala.foldLeft(Map[AnyRef, AnyRef]()) {
    (acc, kv) => acc + ((kv.getKey, kv.getValue))
  }

  def toConf(map: Map[AnyRef, AnyRef]): Configuration = {
    val conf = new Configuration
    map.foreach { x => conf.set(x._1.toString, x._2.toString) }
    conf
  }

  def toJobConf(map: Map[AnyRef, AnyRef]): JobConf = new JobConf(toConf(map))

  /**
    * Converts Tuples from Tap into a Seq of the specified type, in memory --TODO: Unit test.
    */
  def toSeq[T](tap: HadoopTap, job: JobConf)(implicit conv: TupleConverter[T]): Seq[T] = {
    val iter = new HadoopFlowProcess(job).openTapForRead(tap)
    val seq = Seq() ++ (for (entry <- iter.asScala) yield { conv(entry) })
    iter.close()
    seq
  }

  /**
    * Converts Tuples from Tap into a Map with the specified types, in memory --TODO: Unit test.
    */
  def toMap[K, V](tap: HadoopTap, job: JobConf)(implicit conv: TupleConverter[(K, V)]): Map[K, V] = {
    val iter = new HadoopFlowProcess(job).openTapForRead(tap)
    val map = Map() ++ (for (entry <- iter.asScala) yield { conv(entry) })
    iter.close()
    map
  }

  /**
    * Returns a TmpHfs containing the contents of the given sequence of the specified type --TODO: Unit test.
    */
  def toTempTap[T](fields: Fields = Fields.UNKNOWN, seq: Seq[T], job: JobConf)(implicit set: TupleSetter[T]): TmpHfs = {
    val tempTap = new TmpHfs(fields, job)
    val collector = tempTap.openForWrite(new HadoopFlowProcess(job))
    for (item <- seq) collector.add(set(item))
    collector.close
    tempTap
  }

}
