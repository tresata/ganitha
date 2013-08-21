package com.tresata.ganitha.mahout

import java.io.{ DataInputStream, DataOutputStream }

import org.apache.hadoop.conf.Configuration

import org.apache.mahout.math.{ Vector, VectorWritable }

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }

import com.tresata.ganitha.util.ScaldingUtils

object VectorSerializer {

  def register(config : Map[AnyRef, AnyRef]) : Map[AnyRef, AnyRef] =
    config + ("cascading.kryo.hierarchy.registrations" ->
      ScaldingUtils.addToSeparatedStrings(config.get("cascading.kryo.hierarchy.registrations").map(_.toString), ":",
        "org.apache.mahout.math.Vector,com.tresata.ganitha.mahout.VectorSerializer"))

  def register(conf : Configuration) {
    conf.set("cascading.kryo.hierarchy.registrations",
      ScaldingUtils.addToSeparatedStrings(Option(conf.get("cascading.kryo.hierarchy.registrations")), ":",
        "org.apache.mahout.math.Vector,com.tresata.ganitha.mahout.VectorSerializer"))
  }

}

/**
  * Kryo Serializer for Mahout Vector interface.
  */
class VectorSerializer() extends Serializer[Vector] {

  setImmutable(false)

  setAcceptsNull(true)

  def write(kryo: Kryo, output: Output, vector: Vector) {
    VectorWritable.writeVector(new DataOutputStream(output), vector)
  }

  def read(kryo: Kryo, input: Input, cls: Class[Vector]): Vector = VectorWritable.readVector(new DataInputStream(input))

}
