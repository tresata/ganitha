package com.tresata.ganitha.mahout

import org.apache.mahout.math.Vector

/**
  * Provides implicit conversions for Mahout's Vector interface and some function interfaces.
  */
object Implicits {

  implicit def vectorToRichVector(vector : Vector) = new RichVector(vector)

  implicit def richVectorToVector(richVector : RichVector) = richVector.vector

  implicit def functionToDoubleFunction(f : Function1[Double, Double]) = new Function1Wrapper(f)

  implicit def functionToDoubleDoubleFunction(f : Function2[Double, Double, Double]) = new Function2Wrapper(f)

}
