// use nitro-sbt-dev-settings to configure

import com.nitro.build._

import NitroPublishHelpers._

// GAV

lazy val pName  = "kafka_testing"
lazy val semver = NitroSemanticVersion(0,0,0)
organization := NitroGroupId.platform
name := pName
version := semver.toString

// dependencies and their resolvers

libraryDependencies ++= Seq(
  NitroGroupId.platform %% "nitroz-pipeline" % "0.5.8",

  "io.confluent"     % "kafka-avro-serializer" % "1.0" exclude ("org.slf4j", "slf4j-log4j12") exclude ("log4j", "log4j"),
  "org.apache.avro"  % "avro"                  % "1.7.7",
  "org.apache.kafka" %% "kafka"                % "0.8.2.1" % Provided,
  "com.softwaremill" %% "reactive-kafka"       % "0.6.0",
  // Testing
  "org.scalatest" %% "scalatest" % "2.2.4" % Test
)
resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Confluentic repository" at "http://packages.confluent.io/maven/",
  NitroArtifactInfo.releaseResolver
)

// compile & runtime

//                         :::   NOTE   :::
// we want to update to JVM 8 ASAP !
// since we know that we want to be able to use this stuff w/ Spark,
// we unfortunately have to limit ourselves to jvm 7.
// once this gets resolved, we'll update:
// [JIRA Issue]     https://issues.apache.org/jira/browse/SPARK-6152
lazy val devConfig = {
  import CompileScalaJava._
  Config.spark
}
scalaVersion := "2.11.7"
CompileScalaJava.librarySettings(devConfig)
javaOptions := JvmRuntime.settings(devConfig.jvmVer)

// publishing settings

NitroPublish.settings(
  repo = NitroRepository("mgreaves", pName),
  developers =
    Seq(
      NitroDeveloper("mgreaves", "Malcolm", "Greaves"),
      NitroDeveloper("pkinsky",  "Paul",    "Kinsky")
    )
)

// test settings
fork in Test := false
parallelExecution in Test := false

