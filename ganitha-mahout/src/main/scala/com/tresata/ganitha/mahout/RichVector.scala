package com.tresata.ganitha.mahout

import scala.collection.JavaConverters._

import org.apache.mahout.math.{ Vector, DenseVector, RandomAccessSparseVector }

import Implicits._

object RichVector {

  def apply(array : Array[Double]) : Vector = new DenseVector(array)

  def apply(cardinality : Int) : Vector = new RandomAccessSparseVector(cardinality)

  def apply(cardinality : Int, initialCapacity : Int) : Vector = new RandomAccessSparseVector(cardinality, initialCapacity)

  def apply(cardinality : Int, iterable : Iterable[(Int, Double)]) : Vector = {
    val vector = new RandomAccessSparseVector(cardinality, iterable.size)
    vector.updateMany(iterable)
    vector
  }

}

/**
  * Wrapper class for Mahout Vector using the pimp-my-library pattern.
  * Should not be used directly. Instead import com.tresata.ganitha.mahout.Implicits._ and use Mahout Vectors.
  */
class RichVector(val vector : Vector) extends Iterable[Double] {

  def apply(i : Int) : Double = vector.get(i)

  def update(i : Int, d : Double) { vector.set(i, d) }

  def updateMany(iterable : Iterable[(Int, Double)]) { iterable foreach { x => vector.set(x._1, x._2) } }

  def *(d : Double) : Vector = vector.times(d)

  def /(d : Double) : Vector = vector.divide(d)

  def +(d : Double) : Vector = vector.plus(d)

  def -(d : Double) : Vector = vector.plus(-d)

  def *(v : Vector) : Vector = vector.times(v)

  def /(v : Vector) : Vector = copy.assign(v, (_ : Double) / (_ : Double))

  def +(v : Vector) : Vector = vector.plus(v)

  def -(v : Vector) : Vector = vector.minus(v)

  override def iterator : Iterator[Double] = for (element <- vector.iterator.asScala) yield element.get

  private[this] object NonZeroIterable extends Iterable[(Int, Double)] {
    override def iterator : Iterator[(Int, Double)] = for (element <- vector.iterateNonZero.asScala) yield (element.index, element.get)
  }

  def nonZero : Iterable[(Int, Double)] = NonZeroIterable

  def toSparse : Vector = RichVector(vector.size, nonZero).vector

  def copy : Vector = VectorUtil.cloneVector(vector)

  def vectorMap(d : Double) = copy.assign(d)

  def vectorMap(f : Double => Double) = copy.assign(f)

  def vectorMap(v : Vector)(f : (Double, Double) => Double) = copy.assign(v, f)

  def vectorMapNonZero(f : Double => Double) = {
    val newVector = vector.copy
    newVector.updateMany(vector.nonZero.map{ case(k, v) => (k, f(v)) })
    newVector
  }

}
