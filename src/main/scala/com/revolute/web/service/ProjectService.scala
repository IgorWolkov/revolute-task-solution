package com.revolute.web.service

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.revolute.cluster.ClusterClient.{Client, Multi, Single}
import com.revolute.cluster.ClusterNode
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._

class ProjectService(webAppHost: String,
                     webAppPort: Int,
                     clusterSeedNodes: String,
                     clusterHost: String,
                     clusterPort: Int,
                     clusterRetriesDelay: FiniteDuration,
                     clusterMaxRetries: Int,
                     clusterTimeout: Timeout) {

  val config: Config = ConfigFactory.parseString(s"""
          akka.cluster.seed-nodes=[\"$clusterSeedNodes\"]
          akka.remote.netty.tcp.port=$clusterPort
          akka.remote.artery.canonical.port=$clusterPort
          """).withFallback(ConfigFactory.load())

  implicit val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)
  implicit val flowMaterializer: ActorMaterializer = ActorMaterializer()
  import actorSystem.dispatcher

  val clusterNode: ActorRef = actorSystem.actorOf(Props(new ClusterNode(10)), name = "node")

  val clusterClient: ActorRef = actorSystem.actorOf(Props(new Client(clusterNode, clusterRetriesDelay, clusterMaxRetries, Multi(Single()(clusterTimeout)))))

  Http()
    .bindAndHandle(Api.routeV1(clusterClient)(actorSystem.dispatcher), webAppHost, webAppPort) foreach { _ =>
    println(s"Server was successfully started at ${DateTime.now}")
  }

}
