package com.tresata.ganitha.ml.util

import scala.math._

/**
  * Defines necessary vector operations needed for machine-learning algorithms.
  *
  * For any new representation of vectors, an accompanying VectorHelper object is necessary and should provide
  * implementations of the included methods.
  *
  * @tparam V type of incoming vector objects
  */
trait VectorHelper[V] extends Serializable {
  def plus(v1: V, v2: V): V
  def scale(v: V, k: Double): V
  def divBy(v: V, k: Double): V = scale(v, 1.0 / k)
  def toString(v: V): String
  def size(v: V): Int
  def sum(v: V): Double
  def dot(v1: V, v2: V): Double
  def map(v: V, f: Double => Double): V // maps over non-zero elements for sparse representations
}

/**
  * Defines necessary vector operations for application that require a dense representation of vectors.
  *
  * @tparam V type of incoming vector objects
  */
trait DenseVectorHelper[V] extends VectorHelper[V] {
  def iterator(v: V): Iterator[Double]
}

object StrDblMapVectorHelper extends VectorHelper[StrDblMapVector] {
  def plus(v1: StrDblMapVector, v2: StrDblMapVector) = {
    assert(v1.cardinality == v2.cardinality, "Only vectors of the same cardinality can be added together")
    new StrDblMapVector(v1.cardinality, v1.mapping ++ v2.mapping.map { case (key, value) => key -> (value + v1.mapping.getOrElse(key, 0.0)) })
  }
  def scale(v: StrDblMapVector, k: Double) = new StrDblMapVector(v.cardinality, v.mapping.map { case (key, value) => key -> value * k })
  def toString(v: StrDblMapVector) = "(" + v.cardinality + ", {" + v.mapping.values.mkString(", ") + "})"
  def size(v: StrDblMapVector) = v.cardinality
  def sum(v: StrDblMapVector) = v.mapping.values.sum
  def dot(v1: StrDblMapVector, v2: StrDblMapVector) = v1.mapping.map{ case(k, v) => v * v2.mapping.getOrElse(k, 0.0) }.reduce(_ + _)
  def map(v: StrDblMapVector, f: Double => Double) = new StrDblMapVector(v.cardinality, v.mapping.map{ case(k, v) => (k, f(v)) })
  def l1Distance(v1: StrDblMapVector, v2: StrDblMapVector) = MapVectorDistance.l1Distance(v1.mapping, v2.mapping)
  def euclidean(v1: StrDblMapVector, v2: StrDblMapVector) = MapVectorDistance.euclidean(v1.mapping, v2.mapping)
  def cosine(v1: StrDblMapVector, v2: StrDblMapVector) = MapVectorDistance.cosine(v1.mapping, v2.mapping)
}

import org.apache.mahout.math.{ Vector => MahoutVector }
import com.tresata.ganitha.mahout.Implicits._
object MahoutVectorHelper extends DenseVectorHelper[MahoutVector] {
  def plus(v1: MahoutVector, v2: MahoutVector) = v1 + v2
  def scale(v: MahoutVector, k: Double) = v * k
  def toString(v: MahoutVector) = v.asFormatString
  def size(v: MahoutVector) = v.size
  def sum(v: MahoutVector) = v.sum
  def dot(v1: MahoutVector, v2: MahoutVector) = v1.dot(v2)
  def map(v: MahoutVector, f: Double => Double) = v.vectorMapNonZero(f)
  def l1Distance(v1: MahoutVector, v2: MahoutVector): Double = (v1 - v2).norm(1)
  def euclidean(v1: MahoutVector, v2: MahoutVector): Double = (v1 - v2).norm(2)
  def cosine(v1: MahoutVector, v2: MahoutVector): Double = {
    val dotProd = v1.dot(v2)
    if (dotProd < 0.00000001) 1.0 // don't waste calculations on orthogonal vectors or 0
    val denom = v1.norm(2) * v2.norm(2);
    1.0 - abs(dotProd / denom)
  }
  def iterator(v: MahoutVector) = v.toIterator
}

object VectorImplicits {
  implicit val strDblMapVectorHelper: VectorHelper[StrDblMapVector] = StrDblMapVectorHelper
  implicit val mahoutVectorHelper: DenseVectorHelper[MahoutVector] = MahoutVectorHelper
}

// Map-Vector distance functions

import com.twitter.algebird.{ MapAlgebra, Monoid, Ring, Field, Metric }
object MapVectorDistance extends Serializable {
  protected def toDbl[V: Monoid: Metric](v: V): Double = implicitly[Metric[V]].apply(v, Monoid.zero[V])
  protected def l2Norm[K, V: Ring](v: Map[K, V]): V =
    v.foldLeft(Monoid.zero[V]) { (state: V, entry: (K, V)) => Monoid.plus(state, Ring.times(entry._2, entry._2)) }
  def l1Distance[K, V: Monoid: Metric](v1: Map[K, V], v2: Map[K, V]) = Metric.L1Map[K, V].apply(v1, v2)
  def euclidean[K, V: Monoid: Metric](v1: Map[K, V], v2: Map[K, V]) = Metric.L2Map[K, V].apply(v1, v2)
  def cosine[K, V: Field: Metric](v1: Map[K, V], v2: Map[K, V]): Double = {
    if (v1.keySet.intersect(v2.keySet).size == 0) 1.0
    else 1.0 - toDbl(MapAlgebra.dot(v1, v2)) / sqrt(toDbl(l2Norm(v1)) * toDbl(l2Norm(v2)))
  }
}
