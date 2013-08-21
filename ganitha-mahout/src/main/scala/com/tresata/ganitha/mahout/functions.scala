package com.tresata.ganitha.mahout

import org.apache.mahout.math.function.{ DoubleFunction, DoubleDoubleFunction }

class Function1Wrapper(f : Function1[Double, Double]) extends DoubleFunction {
  def apply(arg1 : Double) : Double = f.apply(arg1)
}

class Function2Wrapper(f : Function2[Double, Double, Double]) extends DoubleDoubleFunction {
  def apply(arg1 : Double, arg2 : Double) : Double = f.apply(arg1, arg2)
}

class DoubleFunctionWrapper(f : DoubleFunction) extends Function1[Double, Double] {
  def apply(arg1 : Double) = f.apply(arg1)
}

class DoubleDoubleFunctionWrapper(f : DoubleDoubleFunction) extends Function2[Double, Double, Double] {
  def apply(arg1 : Double, arg2 : Double) = f.apply(arg1, arg2)
}
