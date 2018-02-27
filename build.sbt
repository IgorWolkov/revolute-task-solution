import Dependencies._

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "com.revolute",
      scalaVersion := "2.12.4",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "task-solution",
    mainClass in (Compile, run) := Some("com.revolute.web.service.Boot"),
    resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",
    libraryDependencies += akkaActor,
    libraryDependencies += akkaStream,
    libraryDependencies += akkaHttp,
    libraryDependencies += akkaRemote,
    libraryDependencies += akkaCluster,
    libraryDependencies += akkaMetrics,
    libraryDependencies ++= circe,
    libraryDependencies += akkaHttpCirce,
    libraryDependencies += shapeless,
    libraryDependencies += monix,
    libraryDependencies += logbackClassic,
    libraryDependencies += scalaLogging,
    libraryDependencies += scalactic,
    libraryDependencies += scalatest,
    libraryDependencies += akkaHttpTestkit,


    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
    parallelExecution in ThisBuild := false,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    testForkedParallel in Test := false,
    testForkedParallel in IntegrationTest := false
  )
