package com.tresata.ganitha.ml.util

import scala.math.sqrt
import com.twitter.algebird.{ MapAlgebra, Monoid, Ring, Field, Metric }
import org.apache.mahout.math.{ Vector => MahoutVector }
import com.tresata.ganitha.mahout.Implicits._

/**
  * Defines necessary vector operations needed for machine-learning algorithms.
  *
  * For any new representation of vectors, an accompanying VectorHelper object is necessary and should provide
  * implementations of vector addition, scalar multiplication, and a toString method.
  *
  * @tparam V type of incoming vector objects
  */
trait VectorHelper[V] extends Serializable {
  def plus(v1: V, v2: V): V
  def scale(v: V, k: Double): V
  def divBy(v: V, k: Double): V = scale(v, 1.0 / k)
  def toString(v: V): String
}

object StrDblMapVectorHelper extends VectorHelper[StrDblMapVector] {
  def plus(v1: StrDblMapVector, v2: StrDblMapVector) = v1 ++ v2.map { case (key, value) => key -> (value + v1.getOrElse(key, 0.0)) }
  def scale(v: StrDblMapVector, k: Double) = v.map { case (key, value) => key -> value * k }
  def toString(v: StrDblMapVector) = "(" + v.values.mkString(", ") + ")"
  def l1Distance(v1: StrDblMapVector, v2: StrDblMapVector) = MapVectorDistance.l1Distance(v1, v2)
  def euclidean(v1: StrDblMapVector, v2: StrDblMapVector) = MapVectorDistance.euclidean(v1, v2)
  def cosine(v1: StrDblMapVector, v2: StrDblMapVector) = MapVectorDistance.cosine(v1, v2)
}

object MahoutVectorHelper extends VectorHelper[MahoutVector] {
  def plus(v1: MahoutVector, v2: MahoutVector) = v1 + v2
  def scale(v: MahoutVector, k: Double) = v * k
  def toString(v: MahoutVector) = v.asFormatString
  def l1Distance(v1: MahoutVector, v2: MahoutVector): Double = (v1 - v2).norm(1)
  def euclidean(v1: MahoutVector, v2: MahoutVector): Double = (v1 - v2).norm(2)
  def cosine(v1: MahoutVector, v2: MahoutVector): Double = {
    val dotProd = v1.dot(v2)
    if (dotProd < 0.00000001) 1.0 // don't waste calculations on orthogonal vectors or 0
    val denom = sqrt(v1.getLengthSquared * v2.getLengthSquared);
    1.0 - scala.math.abs(dotProd / denom)
  }
}

object VectorImplicits {
  implicit val strDblMapVectorHelper: VectorHelper[StrDblMapVector] = StrDblMapVectorHelper
  implicit val mahoutVectorHelper: VectorHelper[MahoutVector] = MahoutVectorHelper
}

// Map-Vector distance functions

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
