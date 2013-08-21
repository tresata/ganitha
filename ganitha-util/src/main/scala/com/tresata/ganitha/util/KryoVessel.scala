package com.tresata.ganitha.util

import java.io.{ Serializable, ObjectInputStream, ObjectOutputStream }

import org.apache.hadoop.conf.Configuration

import com.esotericsoftware.kryo.io.{ Input, Output }

import com.twitter.chill.{ KryoSerializer => ChillKryoSerializer }

import cascading.kryo.KryoSerialization

/**
  * Attempt at providing a vessel that can carry objects through the java Serialization nightmare
  * as long as the objects can be serialized with Kryo.
  */
class KryoVessel[T](var load: T, private var conf: Configuration) extends Serializable {
  require(load != null, "load cannot be null")

  private def getKryo = {
    val kryo = new KryoSerialization(conf).populatedKryo
    ChillKryoSerializer.registerAll(kryo)
    kryo
  }

  private var kryo = getKryo

  private def readObject(in: ObjectInputStream) {
    conf = new Configuration()
    conf.readFields(in)
    kryo = getKryo
    load = kryo.readClassAndObject(new Input(in)).asInstanceOf[T]
  }

  private def writeObject(out: ObjectOutputStream) {
    conf.write(out)
    val output = new Output(out);
    kryo.writeClassAndObject(output, load);
    output.flush()
  }

}
