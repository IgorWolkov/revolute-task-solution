package com.revolute.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.util.Timeout
import com.revolute.cluster.ClusterClient
import com.revolute.cluster.ClusterClient.Client
import com.revolute.cluster.model.{client, _}
import com.revolute.cluster.model.external.SuccessMessage
import com.revolute.web.service.Api
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Json, Printer}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.language.postfixOps

class ApiSpec extends WordSpec with Matchers with ScalatestRouteTest {

  val retriesDelay: FiniteDuration = 1 second
  val maxRetries = 10
  implicit val timeout: Timeout = Timeout(20 seconds)
  implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(20 seconds)

  "Web application" should {

    "process account initiation and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Appending error
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // 4 attempts for locking
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      val rout: Route = Api.routeV1(clientActor)(system.dispatcher)

      // Positive flow
      Put(
        "/v1/accounts",
        HttpEntity(`application/json`, AccountInitiation(2, 0, "InitAccount").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.AccountInitiationComplete(AccountInitiation(2, 0, "InitAccount")): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

      // Appending error
      Put(
        "/v1/accounts",
        HttpEntity(`application/json`, AccountInitiation(6, 0, "InitAccount").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.AccountInitiationFailed(AccountInitiation(6, 0, "InitAccount"), _)) => succeed
          case _                                                                                  => fail()
        }
      }

      // 4 attempts for locking
      Put(
        "/v1/accounts",
        HttpEntity(`application/json`, AccountInitiation(10, 0, "InitAccount").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.AccountInitiationComplete(AccountInitiation(10, 0, "InitAccount")): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

    }

    "process bank transfer and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Reading error
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Disapproved
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // One appending error with one revert: see survivorship bias
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Two appending errors with no reverts
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // 4 attempts for locking
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      val rout: Route = Api.routeV1(clientActor)(system.dispatcher)

      // Positive flow
      Post(
        "/v1/accounts/transfer",
        HttpEntity(`application/json`, BankTransfer(1, 2, 0, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.BankTransferComplete(BankTransfer(1, 2, 0, "BankTransfer")): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

      // Reading error
      Post(
        "/v1/accounts/transfer",
        HttpEntity(`application/json`, BankTransfer(3, 4, 10, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.BankTransferFailed(BankTransfer(3, 4, 10, "BankTransfer"), _))  => succeed
          case _                                                                              => fail()
        }
      }

      // Disapproved
      Post(
        "/v1/accounts/transfer",
        HttpEntity(`application/json`, BankTransfer(3, 4, 10, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.BankTransferFailed(BankTransfer(3, 4, 10, "BankTransfer"), _))  => succeed
          case _                                                                              => fail()
        }
      }

      // One appending error with one revert
      Post(
        "/v1/accounts/transfer",
        HttpEntity(`application/json`, BankTransfer(5, 6, 0, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.BankTransferFailed(BankTransfer(5, 6, 0, "BankTransfer"), _))  => succeed
          case _                                                                              => fail()
        }
      }

      // Two appending errors with no reverts
      Post(
        "/v1/accounts/transfer",
        HttpEntity(`application/json`, BankTransfer(8, 9, 0, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.BankTransferFailed(BankTransfer(8, 9, 0, "BankTransfer"), _))  => succeed
          case _                                                                              => fail()
        }
      }

      // 4 attempts for locking
      Post(
        "/v1/accounts/transfer",
        HttpEntity(`application/json`, BankTransfer(10, 11, 0, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.BankTransferComplete(BankTransfer(10, 11, 0, "BankTransfer")): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

    }

    "revert bank transfer and/or handle errors" in {
      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // One appending error
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Two appending errors
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // 4 attempts for locking
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      val rout: Route = Api.routeV1(clientActor)(system.dispatcher)

      // Positive flow
      Post(
        "/v1/accounts/transfer/1",
        HttpEntity(`application/json`, BankTransfer(1, 2, 10, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.BankTransferReversionComplete(1, BankTransfer(1, 2, 10, "BankTransfer")): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

      // One appending error
      Post(
        "/v1/accounts/transfer/1",
        HttpEntity(`application/json`, BankTransfer(5, 6, 10, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.BankTransferReversionFailed(1, BankTransfer(5, 6, 10, "BankTransfer"), _))  => succeed
          case _                                                                                          => fail()
        }
      }

      // Two appending errors
      Post(
        "/v1/accounts/transfer/1",
        HttpEntity(`application/json`, BankTransfer(8, 9, 10, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK

        parse(responseAs[String]).getOrElse(Json.Null).as[external.FailureMessage] match {
          case Right(external.BankTransferReversionFailed(1, BankTransfer(8, 9, 10, "BankTransfer"), _))  => succeed
          case _                                                                                          => fail()
        }
      }

      // 4 attempts for locking
      Post(
        "/v1/accounts/transfer/1",
        HttpEntity(`application/json`, BankTransfer(10, 11, 10, "BankTransfer").asJson.pretty(Printer.noSpaces))
      ) ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.BankTransferReversionComplete(1, BankTransfer(10, 11, 10, "BankTransfer")): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }
    }

    "retrieve records and/or handle errors" in {
      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq(Transaction(7, 19, 42, None, "", Committed)))) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq(Transaction(7, 19, 42, None, "", Reverted)))) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      val rout: Route = Api.routeV1(clientActor)(system.dispatcher)

      // Positive flow
      Get("/v1/accounts/1") ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.RecordsAndBalance(1, external.Balance(1, 0), Seq.empty): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

      Get("/v1/accounts/1") ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.RecordsAndBalance(1, external.Balance(1, 42), Seq(Transaction(7, 19, 42, None, "", Committed))): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }

      // Corrupted records
      Get("/v1/accounts/1") ~> rout ~> check {
        status shouldBe OK
        responseAs[String] shouldBe (external.RecordsAndBalance(1, external.CorruptedBalance(1, s"Corrupted balance: there is no transfer transaction for [1]"), Seq(Transaction(7, 19, 42, None, "", Reverted))): SuccessMessage).asJson.pretty(Printer.noSpaces)
      }
    }
  }


}
