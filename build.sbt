
import sbt.librarymanagement.ConflictWarning

enablePlugins(JavaAppPackaging)
conflictWarning := ConflictWarning.disable
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

name := "databricks-fhir-service"
version := "0.0.1"

lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.5.1")
ThisBuild / organization := "com.databricks.industry.solutions"
ThisBuild / organizationName := "Databricks, Inc."
ThisBuild / resolvers += "Akka library repository".at("https://repo.akka.io/maven")
ThisBuild / scalaVersion := "3.4.2"


libraryDependencies ++= {
  val akkaHttpV      = "10.2.10"
  val akkaV          = "2.6.20"
  val circeV         = "0.14.9"
  val scalaTestV     = "3.2.19"
  val akkaHttpCirceV = "1.39.2"

  Seq(
    "com.databricks"    % "databricks-jdbc" % "2.6.40",
    "joda-time"         % "joda-time"       % "2.12.7",
    "io.circe"          %% "circe-core" % circeV,
    "io.circe"          %% "circe-parser" % circeV,
    "io.circe"          %% "circe-generic" % circeV,
    "org.scalatest"     %% "scalatest" % scalaTestV % "test"
  ) ++ Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "de.heikoseeberger" %% "akka-http-circe" % akkaHttpCirceV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV % "test"
  ).map(_.cross(CrossVersion.for3Use2_13))
}

javaOptions += "--add-opens"
javaOptions += "java.base/java.nio=ALL-UNNAMED"

fork in run := true

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${name.value}-${version.value}." + artifact.extension
}
javacOptions ++= Seq("-source", "17", "-target", "17")

