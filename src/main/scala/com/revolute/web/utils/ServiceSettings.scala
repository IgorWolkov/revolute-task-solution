package com.revolute.web.utils

import akka.util.Timeout
import cats.syntax.either._
import com.revolute.web.model._

import scala.concurrent.duration._

object ServiceSettings extends App {

  lazy val WebAppHost           = "webAppHost"
  lazy val WebAppPort           = "webAppPort"
  lazy val ClusterSeedNodes     = "clusterSeedNodes"
  lazy val ClusterNodeHost      = "clusterNodeHost"
  lazy val ClusterNodePort      = "clusterNodePort"
  lazy val ClusterRetriesDelay  = "clusterRetriesDelay"
  lazy val ClusterMaxRetries    = "clusterMaxRetries"
  lazy val ClusterTimeout       = "clusterTimeout"

  def webAppHost: ErrorsOr[String] =
    Option(System.getProperty(WebAppHost))
      .toRight(s"Web application host [-D$WebAppHost] property is not specified" :: Nil)

  def webAppPort: ErrorsOr[Int] =
    Option(System.getProperty(WebAppPort))
      .toRight(s"Web application port [-D$WebAppPort] property is not specified" :: Nil)
        .flatMap { v =>
          Either.catchOnly[NumberFormatException]{ v.toInt }.leftMap(e => e.getMessage :: Nil)
        }

  def clusterSeedNodes: ErrorsOr[String] =
    Option(System.getProperty(ClusterSeedNodes))
      .toRight(s"Cluster node host [-D$ClusterSeedNodes] property is not specified" :: Nil)


  def clusterNodeHost: ErrorsOr[String] =
    Option(System.getProperty(ClusterNodeHost))
      .toRight(s"Cluster node host [-D$ClusterNodeHost] property is not specified" :: Nil)

  def clusterNodePort: ErrorsOr[Int] =
    Option(System.getProperty(ClusterNodePort))
      .toRight(s"Cluster node port [-D$ClusterNodePort] property is not specified" :: Nil)
      .flatMap { v =>
        Either.catchOnly[NumberFormatException]{ v.toInt }.leftMap(e => e.getMessage :: Nil)
      }

  def clusterRetriesDelay: ErrorsOr[FiniteDuration] =
    Option(System.getProperty(ClusterRetriesDelay))
      .toRight(s"Cluster reties delay [-D$ClusterRetriesDelay] property is not specified" :: Nil)
      .flatMap { v =>
        Either.catchOnly[NumberFormatException]{ v.toInt seconds}.leftMap(e => e.getMessage :: Nil)
      }

  def clusterMaxRetries: ErrorsOr[Int] =
    Option(System.getProperty(ClusterMaxRetries))
      .toRight(s"Cluster max retries number [-D$ClusterMaxRetries] property is not specified" :: Nil)
      .flatMap { v =>
        Either.catchOnly[NumberFormatException]{ v.toInt }.leftMap(e => e.getMessage :: Nil)
      }

  def clusterTimeout: ErrorsOr[Timeout] =
    Option(System.getProperty(ClusterTimeout))
      .toRight(s"Cluster timeout [-D$ClusterTimeout] property is not specified" :: Nil)
      .flatMap { v =>
        Either.catchOnly[NumberFormatException]{ Timeout(v.toInt seconds)}.leftMap(e => e.getMessage :: Nil)
      }
}
