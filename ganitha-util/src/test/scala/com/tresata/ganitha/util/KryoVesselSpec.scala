package com.tresata.ganitha.util

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import org.apache.hadoop.conf.Configuration

import org.scalatest.FunSpec

object KryoVesselSpec {

  case class TestCase(name : String)

  def roundTrip[T](t : T) = {
    val vessel = new KryoVessel(t, new Configuration())
    val out = new ByteArrayOutputStream()
    val objectOut = new ObjectOutputStream(out)
    objectOut.writeObject(vessel)
    val in = new ByteArrayInputStream(out.toByteArray)
    val y = new ObjectInputStream(in).readObject().asInstanceOf[KryoVessel[T]].load
    y
  }

}

class KryoVesselSpec extends FunSpec {
  import KryoVesselSpec._

  describe("A KryoVessel") {

    it("should be able to serialize and deserialize lists") {
      val x = List(1, 2, 3)
      assert(x == roundTrip(x))
    }

    it("should be able to serialize and deserialize a basic new class that does not extend serializable") {
      val x = TestCase("i am x")
      assert(x == roundTrip(x))
    }

  }

}
