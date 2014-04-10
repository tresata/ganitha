import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object GanithaBuild extends Build {

  val sharedSettings = Project.defaultSettings ++ assemblySettings ++ Seq(
    organization := "com.tresata",
    scalaVersion := "2.10.4",
    retrieveManaged := true,
    retrievePattern := "[artifact](-[revision])(-[classifier]).[ext]",
    libraryDependencies ++= Seq(
      "com.twitter" %% "scalding-core" % "0.9.0rc4",
      "org.apache.hadoop" % "hadoop-core" % "1.0.4" % "provided",
      "cascading.kryo" % "cascading.kryo" % "0.4.6" % "compile",
      "org.scalatest" %% "scalatest" % "1.9.2" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.6.1" % "test",
      "org.scalanlp" % "breeze_2.10" % "0.7" % "provided",
      "org.jblas" % "jblas" % "1.2.3" % "provided",
      "org.scala-saddle" % "saddle-core_2.10" % "1.3.2" % "provided"
    ),
    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com"
    ),
    test in assembly := {},
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
      case s if s.endsWith(".class") => MergeStrategy.last
      case s if s.endsWith("project.clj") => MergeStrategy.concat
      case s if s.endsWith(".html") => MergeStrategy.last
      case x => old(x)
    }}
  )

  lazy val ganitha = Project(
    id = "ganitha",
    base = file("."),
    settings = sharedSettings
  ).settings(
    test := { },
    publish := { },
    publishLocal := { }
  ).aggregate(ganithaUtil, ganithaMahout, ganithaMl)

  lazy val ganithaUtil = Project(
    id = "ganitha-util",
    base = file("ganitha-util"),
    settings = sharedSettings
  )

  lazy val ganithaMahout = Project(
    id = "ganitha-mahout",
    base = file("ganitha-mahout"),
    settings = sharedSettings
  ).settings(
    libraryDependencies ++= Seq(
      "org.apache.mahout" % "mahout-math" % "0.7",
      "org.apache.mahout" % "mahout-core" % "0.7"
    )
  ).dependsOn(ganithaUtil)

  lazy val ganithaMl = Project(
    id = "ganitha-ml",
    base = file("ganitha-ml"),
    settings = sharedSettings
  ).dependsOn(ganithaUtil, ganithaMahout)

}
