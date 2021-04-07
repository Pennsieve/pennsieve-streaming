package com.pennsieve.streaming.server

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ Directives, Route }
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.streaming.server.TSJsonSupport._

import scala.util.{ Failure, Success }

class MontageValidationService(
  claim: Claim
)(implicit
  ports: WebServerPorts
) extends Directives
    with TSJsonSupport {

  def route(packageId: String): Route =
    get {
      onComplete(ports.getChannels(packageId, claim).value) {
        case Failure(e) => complete(StatusCodes.InternalServerError, e)
        case Success(channelsResult) =>
          channelsResult
            .flatMap {
              case (channels, _) =>
                Montage.validateAllMontages(channels.map(_.name))
            }
            .fold(err => complete(err.statusCode, err), _ => complete(StatusCodes.OK))
      }
    }
}
