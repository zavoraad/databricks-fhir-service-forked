import sbt.librarymanagement.ConflictWarning
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule
import com.typesafe.sbt.packager.docker._


enablePlugins(DockerPlugin, GitVersioning)
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
  val grpcV          = "1.76.1"

  Seq(
    "com.databricks"    % "databricks-jdbc" % "2.6.40",
    "com.databricks"    % "zerobus-ingest-sdk" % "0.1.0",
    //"com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    //"com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0",
    "joda-time"         % "joda-time"       % "2.12.7",
    "ch.qos.logback"    % "logback-classic"  % "1.5.25",
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
    "org.scalatest"     %% "scalatest" % scalaTestV % "test",
    "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.4" % "test",
    "com.dimafeng" %% "testcontainers-scala-core" % "0.41.4" % "test",
    "com.softwaremill.sttp.client3" %% "core" % "3.9.7" % "test",
    "com.softwaremill.sttp.client4" %% "core" % "4.0.15",
    "com.softwaremill.sttp.client4" %% "circe" % "4.0.15",
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
assembly / assemblyJarName := "databricks-fhir-service.jar"
assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) =>
      xs map { _.toLowerCase } match {
        case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
          MergeStrategy.discard
        case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.concat
        case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
          MergeStrategy.concat
        case _ => MergeStrategy.first
      }
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("application.conf") => MergeStrategy.concat
    case x => MergeStrategy.first
}


// Docker configuration using assembly fat JAR
Docker / packageName := "databricks-fhir-api"
Docker / version := "latest"
Docker / dockerAliases := {
  val gitHash = git.gitHeadCommit.value.getOrElse("unknown")
  val registry = dockerRepository.value
  val name = (Docker / packageName).value
  Seq(
    dockerAlias.value.withTag(Some("latest")),
    dockerAlias.value.withTag(Some(gitHash))
  )
}

// Map the assembly JAR into the Docker build context
Docker / mappings := {
  val jar = assembly.value
  Seq(jar -> s"target/scala-3.4.2/${jar.getName}")
}

// Configure exposed ports
dockerExposedPorts := Seq(9000)

addCommandAlias("testDocker", ";assembly; docker:publishLocal; testOnly *DockerIntegrationTest")