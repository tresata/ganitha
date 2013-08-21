package com.tresata.ganitha.util

import org.apache.hadoop.mapred.{ JobConf, RecordReader, OutputCollector }

import cascading.flow.Flow
import cascading.tap.Tap
import cascading.scheme.Scheme

object `package` {
  type HadoopTap = Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]
  type HadoopFlow = Flow[JobConf]
  type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]
}
