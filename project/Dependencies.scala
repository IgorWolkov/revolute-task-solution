import sbt._

object Dependencies {
  val akkaVersion = "2.5.9"
  val circeVersion = "0.8.0"
  val sttpVersion  = "1.1.4"

  lazy val akkaActor      = "com.typesafe.akka"           %% "akka-actor"             % akkaVersion
  lazy val akkaStream     = "com.typesafe.akka"           %% "akka-stream"            % akkaVersion
  lazy val akkaRemote     = "com.typesafe.akka"           %% "akka-remote"            % akkaVersion
  lazy val akkaCluster    = "com.typesafe.akka"           %% "akka-cluster"           % akkaVersion
  lazy val akkaMetrics    = "com.typesafe.akka"           %% "akka-cluster-metrics"   % akkaVersion
  lazy val akkaHttp       = "com.typesafe.akka"           %% "akka-http"              % "10.0.11"
  lazy val akkaHttpCirce  = "de.heikoseeberger"           %% "akka-http-circe"        % "1.18.1"
  lazy val shapeless      = "com.chuusai"                 %% "shapeless"              % "2.3.3"
  lazy val monix          = "io.monix"                    %% "monix"                  % "2.3.3"
  lazy val scalaLogging   = "com.typesafe.scala-logging"  %% "scala-logging"          % "3.7.2"
  lazy val logbackClassic = "ch.qos.logback"              % "logback-classic"         % "1.2.3"

  lazy val scalactic            = "org.scalactic"      %% "scalactic"               % "3.0.4"
  lazy val scalatest            = "org.scalatest"      %% "scalatest"               % "3.0.4"     % Test
  lazy val akkaHttpTestkit      = "com.typesafe.akka"  %% "akka-http-testkit"       % "10.0.11"   % Test

  lazy val circe = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)

}
