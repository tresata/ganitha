package com.tresata.ganitha.mahout

import org.apache.mahout.math.{ Vector, DenseVector }

import org.scalatest.FunSpec

import Implicits._

class RichVectorSpec extends FunSpec {

  describe("A RichVector") {

    it("should support apply constructors") {
      assert(RichVector(6, List((1, 1.0), (3, 2.0), (5, 3.0))) == RichVector(Array(0.0, 1.0, 0.0, 2.0, 0.0, 3.0)))
    }

    it("should support getters and setters") {
      val vector0 = RichVector(Array(1.0, 2.0, 3.0))
      vector0(1) = 5.0
      assert(vector0(1) == 5.0)
      vector0.updateMany(List((1, 1.0), (2, 2.0)))
      assert(vector0 == RichVector(Array(1.0, 1.0, 2.0)))
    }

    it("should support aggregation") {
      val vector0 = new DenseVector(Array(1.0, 2.0, 3.0))
      val vector1 = new DenseVector(Array(4.0, 5.0, 6.0))

      assert(vector0.aggregate((_ : Double) + (_ : Double), math.pow(_ : Double, 2)) == 14)
      assert(vector0.aggregate(vector1, (_ : Double) + (_ : Double), (_ : Double) * (_ : Double)) == 32)

      assert(vector0.nonZero.map((x : (Int, Double)) => x._2 * x._2).sum == 14)
      assert(vector0.vectorMap(vector1){ _ * _}.fold(0.0){_ + _} == 32)
    }

    it("should support scalar operations") {
      val vector0 = new DenseVector(Array(1.0, 2.0, 3.0))
      assert(((vector0 * 2) / 2) == vector0)
      assert(((vector0 + 2) - 2) == vector0)
    }

    it("should support vector operations") {
      val vector0 = new DenseVector(Array(1.0, 2.0, 3.0))
      val vector1 = new DenseVector(Array(4.0, 5.0, 6.0))
      assert(vector0 + vector1 - vector0 == vector1)
      assert((vector0 * vector1) / vector1 == vector0)
    }

    it("should support iterable operations") {
      val vector0 = new DenseVector(Array(1.0, 2.0, 3.0))
      assert(vector0.count(_ > 1.0) == 2)
      assert(vector0.foldLeft(0D)(_ + _) == 6.0)
      assert(vector0.nonZero.foldLeft(0D)(_ + _._2) == 6.0)
    }

    it("should support map-like operations") {
      val vector0 = new DenseVector(Array(1.0, 2.0, 3.0))
      assert(vector0.map((_ : Double) * 2) sameElements new DenseVector(Array(2.0, 4.0, 6.0)))
      assert(vector0.vectorMap(_ * 2) == new DenseVector(Array(2.0, 4.0, 6.0)))
    }

  }

}
