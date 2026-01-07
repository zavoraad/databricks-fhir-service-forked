import sbt.librarymanagement.ConflictWarning
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)
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
  val grpcV          = "1.68.1"

  Seq(
    "com.databricks"    % "databricks-jdbc" % "2.6.40",
    "com.databricks"    % "zerobus-ingest-sdk" % "0.1.0",
    //"com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    //"com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0",
    "joda-time"         % "joda-time"       % "2.12.7",
    "ch.qos.logback"    % "logback-classic"  % "1.5.18",
    "io.circe"          %% "circe-core" % circeV,
    "io.circe"          %% "circe-parser" % circeV,
    "io.circe"          %% "circe-generic" % circeV,
    "com.lihaoyi" %% "upickle" % "4.1.0",
    "com.zaxxer" % "HikariCP" % "6.3.0",
    "com.google.protobuf" % "protobuf-java" % "4.33.0",
    "com.google.protobuf" % "protobuf-java-util" % "4.33.0",
    "io.grpc" % "grpc-netty-shaded" % grpcV,
    "io.grpc" % "grpc-protobuf" % grpcV,
    "io.grpc" % "grpc-stub" % grpcV,
    "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5",
    "org.scalatest"     %% "scalatest" % scalaTestV % "test"
  ) ++ Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "de.heikoseeberger" %% "akka-http-circe" % akkaHttpCirceV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV % "test"
  ).map(_.cross(CrossVersion.for3Use2_13))
}


lazy val arrowJVMOptions = Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

run / fork := true
run / javaOptions ++= arrowJVMOptions

reStart / javaOptions ++= arrowJVMOptions

Test / fork := true
Test / javaOptions ++= arrowJVMOptions

Compile / PB.targets := Seq(
  PB.gens.java -> (Compile / sourceManaged).value
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${name.value}-${version.value}." + artifact.extension
}
javacOptions ++= Seq("-source", "17", "-target", "17")

assembly / mainClass := Some("com.databricks.industry.solutions.fhirapi.FhirService")
assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("application.conf") => MergeStrategy.concat
    case x => MergeStrategy.first
}


enablePlugins(GitVersioning)
import sbt.Package.ManifestAttributes

Docker / packageName := "databricks-fhir-api"
Docker / dockerExposedPorts := Seq(9000) //expose port 9000 in the docker image
Docker / version := git.gitHeadCommit.value.get
