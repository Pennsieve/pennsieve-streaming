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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{ BadRequest, Unauthorized }
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.{ Directives, Route }
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import cats.instances.future._
import com.pennsieve.auth.middleware.Jwt.{ Claim, Token }
import com.pennsieve.auth.middleware.{ Jwt, ServiceClaim }
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.{ QuerySequencer, WsClient }
import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.TimeSeriesFlow.{ SessionFilters, SessionMontage }
import com.pennsieve.streaming.server.discover.DiscoverGetChannelsQuery
import com.pennsieve.streaming.{ RangeLookUp, UnitRangeLookUp }
import com.typesafe.config.Config
import scalikejdbc.DBSession
import uk.me.berndporr.iirj.Cascade

import scala.collection.JavaConverters._
import scala.collection.concurrent

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

  // A global map of session ids to filters that are active on
  // specific channels during that session
  val sessionFilters: SessionFilters =
    new ConcurrentHashMap[String, concurrent.Map[String, Cascade]]().asScala

  // A global map of session ids to the montages that are active on
  // specific packages during that session
  val sessionMontages: SessionMontage =
    new ConcurrentHashMap[String, concurrent.Map[String, MontageType]]().asScala

  implicit val jwtConfig: Jwt.Config = getJwtConfig(config)

  val queryLimit = config.getLong("timeseries.query-limit")

  val defaultGapThreshold = config.getDouble("timeseries.default-gap-threshold")

  val rangeLookUp =
    new RangeLookUp(ports.rangeLookupQuery, config.getString("timeseries.s3-base-url"))

  val unitRangeLookUp =
    new UnitRangeLookUp(ports.unitRangeLookupQuery, config.getString("timeseries.s3-base-url"))

  implicit val ec = system.dispatchers.lookup("custom-io-dispatcher")

  val connectionCounter = new AtomicLong()
  val startupTime = System.currentTimeMillis()

  val querySequencer =
    new QuerySequencer(rangeLookUp, unitRangeLookUp)

  val getChannelsQuery: GetChannelsQuery = new GetChannelsQueryImpl()
  val discoverGetChannelsQuery: GetChannelsQuery = new DiscoverGetChannelsQuery()

  def timeseriesQuery(
    claim: Claim,
    getChannelsQuery: GetChannelsQuery
  )(
    packageId: String,
    startAtEpochParam: Option[String]
  ): Route = {
    val flow = getChannelsQuery.query(packageId, claim).map {
      case (channels, logContext) =>
        val cmap = channels.map { c =>
          c.nodeId -> c
        }.toMap

        val session = Jwt.generateToken(claim).value
        // initialize the filters for this session with an empty map
        val channelFilters: concurrent.Map[String, Cascade] =
          new ConcurrentHashMap[String, Cascade]().asScala
        sessionFilters.putIfAbsent(session, channelFilters)

        // initialize the montage for this session
        val packageMontages: concurrent.Map[String, MontageType] =
          new ConcurrentHashMap[String, MontageType]().asScala
        sessionMontages.putIfAbsent(session, packageMontages)

        val startAtEpoch =
          startAtEpochParam.exists(_.toLowerCase.trim() == "true")

        val tsFlow = new TimeSeriesFlow(
          session,
          sessionFilters,
          sessionMontages,
          channelMap = cmap,
          rangeLookup = rangeLookUp,
          unitRangeLookUp = unitRangeLookUp,
          startAtEpoch = startAtEpoch
        )
        connectionCounter.incrementAndGet()
        Flow
          .fromGraph(tsFlow.flowGraph)
          .alsoTo(Sink.onComplete { _ =>
            log.context
              .info(s"Graph complete for session: $session")(logContext)
            connectionCounter.decrementAndGet()
          })
    }
    TimeSeriesFlow.routeFlow(flow)
  }

  def healthCheck: Route = {
    val current = System.currentTimeMillis()
    val age = current - startupTime
    complete(HealthCheck(connectionCounter.get(), age, current))
  }

  type ClaimToRoute = Claim => Route

  val segmentQuery: Route =
    new SegmentService(rangeLookUp, defaultGapThreshold).route
  val continuousQuery: ClaimToRoute =
    claim => new ContinuousQueryService(querySequencer, queryLimit, claim).route
  val unitQuery: ClaimToRoute =
    claim => new UnitQueryService(querySequencer, queryLimit, claim).route

  val validateMontage: MontageValidationService = new MontageValidationService()

  def claimToRoutes(claim: Claim): Route = {
    concat(claimToDiscoverRoutes(claim), pathPrefix("ts") {
      claimToTimeSeriesRoutes(claim, getChannelsQuery)
    })
  }

  def queryRoute(claim: Claim, getChannelsQuery: GetChannelsQuery): Route =
    path("query") {
      parameter('package, 'startAtEpoch ?)(timeseriesQuery(claim, getChannelsQuery))
    }

  def retrieveRoutes(claim: Claim): Route = {
    concat(continuousQuery(claim), unitQuery(claim), concat(segmentQuery))
  }

  def validateMontageRoute(claim: Claim, getChannelsQuery: GetChannelsQuery): Route = {
    path("validate-montage") {
      parameter('package)(validateMontage.route(claim, getChannelsQuery))
    }
  }

  def claimToDiscoverRoutes(claim: Claim): Route = {
    pathPrefix("discover" / "ts") {
      claimToTimeSeriesRoutes(claim, discoverGetChannelsQuery)
    }
  }

  def claimToTimeSeriesRoutes(claim: Claim, getChannelsQuery: GetChannelsQuery): Route = {
    concat(
      queryRoute(claim, getChannelsQuery),
      pathPrefix("retrieve")(retrieveRoutes(claim)),
      validateMontageRoute(claim, getChannelsQuery)
    )

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

          case Right(claim) => claimToRoutes(claim)

          case Left(_) => complete(HttpResponse(BadRequest))
        }

      case _ => noClaimRoutes() ~ complete(HttpResponse(Unauthorized))
    }
}
