val scala213Version = "2.13.8"
val scala3Version   = "3.1.2"

lazy val mavenSnapshots =
  "apache.snapshots" at "https://repository.apache.org/content/groups/snapshots"

resolvers ++= Seq(mavenSnapshots)

val flinkVersion = "1.16.1"

val flinkLibs = Seq(
  "org.apache.flink" % "flink-streaming-java"       % flinkVersion,
  "org.apache.flink" % "flink-core"                 % flinkVersion,
  "org.apache.flink" % "flink-statebackend-rocksdb" % flinkVersion,
  "org.apache.flink" % "flink-test-utils"           % flinkVersion % Test
)

val otherLibs = Seq(
  "org.typelevel" %% "cats-core" % "2.9.0"
)

val testingLibs = Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

lazy val root = project
  .in(file("."))
  .settings(
    name               := "flink4s",
    scalaVersion       := scala3Version,
    crossScalaVersions := Seq(scala3Version, scala213Version),
    libraryDependencies ++= flinkLibs ++ otherLibs ++ testingLibs,
    publishingSettings,
    Test / parallelExecution := false
  )

import ReleaseTransformations.*

lazy val publishingSettings = Seq(
  organization         := "com.rzk91",
  organizationName     := "rzk91",
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/rzk91/flink4s"),
      "scm:git@github.com:rzk91/flink4s.git"
    )
  ),
  description := "Scala 2.13/3 wrapper for Apache Flink",
  homepage    := Some(url("https://github.com/rzk91/flink4s")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  releaseProcess := Seq[ReleaseStep](
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
    pushChanges
  )
)
