package com.revolute.web.service

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.revolute.cluster.model._
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Encoder
import io.circe.generic.auto._
import akka.pattern.ask
import akka.util.Timeout
import com.revolute.cluster.model.external.PerformBankTransfer

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object Api extends LazyLogging {

  implicit val timeout: Timeout = Timeout(30 seconds)

  implicit private def completeFuture[T <: external.ExternalMessage](f: Future[T])(implicit encoder: Encoder[T]): Route = {
    onComplete(f) {
      case Success(v) =>
        v match {
          case success: external.SuccessMessage =>
            logger info s"Request complete: [$success]"
            complete(success)

          case failure: external.FailureMessage =>
            logger warn s"Request failed: [$failure]"
            complete(failure)

          case unexpected =>
            logger error s"Unexpected response  [$unexpected]"
            complete(InternalServerError)
        }

      case Failure(error) =>
        logger.error(s"Internal error: [${error.getMessage}]")
        complete(InternalServerError)
    }
  }

  /**
    * Api versioning
    */
  def routeV1(clusterClient: ActorRef)(implicit context: ExecutionContext): Route =
    pathPrefix("v1" / "accounts") {
      path("transfer" / LongNumber) { transactionId =>
        post {
          entity(as[BankTransfer]) { bankTransfer =>
            // Dummy typification
            (clusterClient ? external.RevertBankTransfer(transactionId, bankTransfer)) map {
              case message: external.ExternalMessage => message
            }
          }
        }
      } ~
        path("transfer") {
          post {
            entity(as[BankTransfer]) { bankTransfer =>
              // Dummy typification
              (clusterClient ? PerformBankTransfer(bankTransfer)) map {
                case message: external.ExternalMessage => message
              }
            }
          }
      } ~
      put {
        entity(as[AccountInitiation]) { accountInitiation =>
          // Dummy typification
          (clusterClient ? external.InitAccount(accountInitiation)) map {
            case message: external.ExternalMessage => message
          }
        }
      } ~
      path(LongNumber) { accountId =>
        get {
          // Dummy typification
          (clusterClient ? external.GetRecords(accountId)) map {
            case message: external.ExternalMessage => message
          }
        }
      }
    }
}
