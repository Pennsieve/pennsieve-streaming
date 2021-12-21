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

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{ BadRequest, Unauthorized }
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.{ Directives, Route }
import akka.actor.ActorSystem
import com.pennsieve.auth.middleware.Jwt.{ Claim, Token }
import com.pennsieve.auth.middleware.{ Jwt, ServiceClaim }
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.WsClient
import com.pennsieve.streaming.server.TSJsonSupport._
import com.typesafe.config.Config
import scalikejdbc.DBSession

class WebServer(
  implicit
  log: ContextLogger,
  ports: WebServerPorts,
  system: ActorSystem,
  wsClient: WsClient,
  config: Config,
  dbSession: DBSession
) extends Directives
    with TSJsonSupport {

  implicit val jwtConfig: Jwt.Config = getJwtConfig(config)

  implicit val ec = system.dispatchers.lookup("custom-io-dispatcher")

  val startupTime = System.currentTimeMillis()

  val timeSeriesRoutes: TimeSeriesRoutes = new TimeSeriesRoutes()

  def healthCheck: Route = {
    val current = System.currentTimeMillis()
    val age = current - startupTime
    complete(HealthCheck(timeSeriesRoutes.getConnectionCount, age, current))
  }

  def noClaimRoutes(): Route =
    pathPrefix("ts") {
      path("health") {
        healthCheck
      }
    }

  def route: Route =
    extractCredentials {
      case Some(OAuth2BearerToken(token)) =>
        Jwt.parseClaim(Token(token)) match {
          case Right(Claim(ServiceClaim(_), _, _)) =>
            complete(HttpResponse(Unauthorized))

          case Right(claim) => timeSeriesRoutes.routes(claim)

          case Left(_) => complete(HttpResponse(BadRequest))
        }

      case _ => noClaimRoutes() ~ complete(HttpResponse(Unauthorized))
    }
}
