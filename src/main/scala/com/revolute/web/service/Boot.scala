package com.revolute.web.service

import akka.util.Timeout
import cats.syntax.either._
import com.revolute.web.utils.ServiceSettings
import com.revolute.web.model._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.FiniteDuration

object Boot extends App with LazyLogging {

  /**
    * Starting all services
    * with validated context
    */
  initContexts().leftMap { errors =>
    logger error s"Cannot start web application: [${errors mkString ", "}]"
    System.exit(1)
  } foreach {
    case (webAppHost, webAppPort, clusterSeedNodes, clusterNodeHost, clusterNodePort, clusterRetriesDelay, clusterMaxRetries, clusterTimeout) =>
      new ProjectService(webAppHost, webAppPort, clusterSeedNodes, clusterNodeHost, clusterNodePort, clusterRetriesDelay, clusterMaxRetries, clusterTimeout)
      logger info s"Service was successfully started"
  }

  /**
    * Accumulating all possible configuration errors in parallel
    */
  def initContexts(): ErrorsOr[(String, Int, String, String, Int, FiniteDuration, Int, Timeout)] = {
    import cats.implicits._

    ( ServiceSettings.webAppHost.toValidated |@|
      ServiceSettings.webAppPort.toValidated |@|
      ServiceSettings.clusterSeedNodes.toValidated |@|
      ServiceSettings.clusterNodeHost.toValidated |@|
      ServiceSettings.clusterNodePort.toValidated |@|
      ServiceSettings.clusterRetriesDelay.toValidated |@|
      ServiceSettings.clusterMaxRetries.toValidated |@|
      ServiceSettings.clusterTimeout.toValidated)
      // TODO: Ugly, find a way to improve
      .map((wah, wap, csn, ch, cp, crd, cmr, cto) => (wah, wap, csn, ch, cp, crd, cmr, cto)).toEither
  }

}
