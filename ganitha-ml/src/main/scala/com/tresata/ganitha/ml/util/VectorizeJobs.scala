package com.tresata.ganitha.ml.util

import cascading.tuple.Fields
import cascading.pipe.Pipe

import com.twitter.scalding.{ Args, Job, SequenceFile, Csv }

import com.tresata.ganitha.util._

class VectorizeJob(args: Args) extends Job(args) {
  val vectorTap = Csv(args("input"), skipHeader = true)
  val idField = args("id")
  val vectorFields = new Fields(args.list("features"): _*)
  val sinkTap = SequenceFile(args("vectors"), ('id, 'vector))
  vectorTap.read
    .then((pipe: Pipe) => vectorizeFields(pipe, idField, vectorFields))
    .write(sinkTap)
}
