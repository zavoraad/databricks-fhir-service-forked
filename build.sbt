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

  Seq(
    "com.databricks"    % "databricks-jdbc" % "2.6.40",
    "joda-time"         % "joda-time"       % "2.12.7",
    "io.circe"          %% "circe-core" % circeV,
    "io.circe"          %% "circe-parser" % circeV,
    "io.circe"          %% "circe-generic" % circeV,
    "com.lihaoyi" %% "upickle" % "4.1.0",
    "com.zaxxer" % "HikariCP" % "5.1.0",
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

run / fork := true

// check this
javaOptions in run += "--add-opens=java.base/java.nio=ALL-UNNAMED"

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

// Use assembly option to relocate Akka packages
assembly / assemblyOption := (assembly / assemblyOption).value.withScalaModuleInfo(false)

// Create relocations for Akka packages
ThisBuild / assemblyMergeStrategy := {
  case x if x.contains("reference.conf") || x.contains("application.conf") => MergeStrategy.concat
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// Assembly shade rules for Akka
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("akka.**" -> "akka.@1")
    .inAll
    .exclude("reference.conf")
    .exclude("application.conf"),
  ShadeRule.rename("com.typesafe.**" -> "com.typesafe.@1")
    .inAll
    .exclude("reference.conf")
    .exclude("application.conf")
)

enablePlugins(GitVersioning)
import sbt.Package.ManifestAttributes

Docker / packageName := "databricks-fhir-api"
Docker / dockerExposedPorts := Seq(9000) //expose port 9000 in the docker image
Docker / version := git.gitHeadCommit.value.get
