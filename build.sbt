import Dependencies._

ThisBuild / scalaVersion     := "2.12.20"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "ch.epfl.scala"
ThisBuild / organizationName := "runner"

lazy val root = (project in file("."))
  .settings(
    name := "lsh-test",
    libraryDependencies ++= Seq(
      munit % Test,
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "org.scalatest" %% "scalatest-funsuite" % "3.2.12" % Test,
      "org.apache.spark" %% "spark-core" % "3.5.5",
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "org.apache.spark" %% "spark-mllib" % "3.5.5",
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
