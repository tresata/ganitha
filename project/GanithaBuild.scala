import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object GanithaBuild extends Build {

  val sharedSettings = Project.defaultSettings ++ assemblySettings ++ Seq(
    organization := "com.tresata",
    scalaVersion := "2.9.3",
    version := "0.1-SNAPSHOT",
    crossScalaVersions := Seq("2.9.3", "2.10.3"),
    retrieveManaged := true,
    retrievePattern := "[artifact](-[revision])(-[classifier]).[ext]",
    libraryDependencies ++= Seq(
      "com.twitter" %% "scalding-core" % "0.9.0rc4",
      "org.apache.hadoop" % "hadoop-core" % "1.0.4" % "provided",
      "cascading.kryo" % "cascading.kryo" % "0.4.6" % "compile",
      "org.scalatest" %% "scalatest" % "1.9.2" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.6.1" % "test"
    ),
    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Clojars Repository" at "http://clojars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com"
    ) ++ Seq(
      "tresata-snapshots" at "http://server01:8080/archiva/repository/snapshots",
      "tresata-releases" at "http://server01:8080/archiva/repository/internal"
    ),
    test in assembly := {},
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
      case s if s.endsWith(".class") => MergeStrategy.last
      case s if s.endsWith("project.clj") => MergeStrategy.concat
      case s if s.endsWith(".html") => MergeStrategy.last
      case x => old(x)
    }},
    publishTo <<= version { (v: String) =>
      val tresata = "http://server01:8080/archiva/repository/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("tresata-snapshots" at tresata + "snapshots")
      else
        Some("tresata-releases"  at tresata + "internal")
    },
    credentials += Credentials(Path.userHome / ".m2" / "credentials_internal"),
    credentials += Credentials(Path.userHome / ".m2" / "credentials_snapshots"),
    credentials += Credentials(Path.userHome / ".m2" / "credentials_proxy")
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
