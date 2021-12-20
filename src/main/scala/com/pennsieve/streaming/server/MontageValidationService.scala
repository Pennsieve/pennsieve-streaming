/*
 * Copyright 2021 University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pennsieve.streaming.server

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ Directives, Route }
import cats.implicits._
import com.pennsieve.auth.middleware.Jwt.Claim

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class MontageValidationService(
  claim: Claim
)(implicit
  ports: WebServerPorts,
  ec: ExecutionContext
) extends Directives
    with TSJsonSupport {

  def route(packageOrgId: Option[Int])(packageId: String): Route =
    get {
      onComplete(ports.getChannels(packageId, claim, packageOrgId).value) {
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

  def discoverRoute(packageId: String): Route = {
    val forResult = for {
      packageOrgId <- ports.discoverApiClient.getOrganizationId(packageId)
      channelsResult <- ports
        .getChannels(packageId, claim, Some(packageOrgId))
    } yield channelsResult

    get {
      onComplete(forResult.value) {
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

}
