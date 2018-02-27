package com.revolute.web

object model {

  trait BusinessError
  object NotFoundError extends BusinessError
  object ExternalServerError extends BusinessError
  object ParsingError extends BusinessError
  object UnexpectedError extends BusinessError

  object ServiceId {
    def apply(serviceId: String) = new ServiceId(Option(serviceId))
  }
  case class ServiceId(serviceId: Option[String])

  object TraceId {
    def apply(traceId: String) = new TraceId(Option(traceId))
  }
  case class TraceId(traceId: Option[String])

  case class BusinessRequest[T](payload: T, serviceId: ServiceId, traceId: TraceId) {
    def map[V](f: T => V): BusinessRequest[V] =
      BusinessRequest(f(payload), serviceId, traceId)
  }

  case class BusinessResponse[T](payload: ErrorOr[T], serviceId: ServiceId, traceId: TraceId) {
    def map[V](f: ErrorOr[T] => ErrorOr[V]): BusinessResponse[V] =
      BusinessResponse(f(payload), serviceId, traceId)
  }

  type ErrorOr[A] = Either[BusinessError, A]

  type ErrorsOr[A] = Either[List[String], A]

  case class Project(id: String, description: String)

  case class Query(q: Option[String] = None) {
    def toQueryString: String = q map { v => s"q=$v" } getOrElse ""
  }

}
