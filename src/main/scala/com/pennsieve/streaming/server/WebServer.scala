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
import akka.http.scaladsl.model.StatusCodes.{ BadRequest, NotFound, Unauthorized }
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.{ Directives, Route }
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import cats.instances.future._
import com.pennsieve.auth.middleware.Jwt.{ Claim, Token }
import com.pennsieve.auth.middleware.{ Jwt, ServiceClaim }
import com.pennsieve.models.PackageType.TimeSeries
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.clients.{
  Error,
  Id,
  NotTimeSeries,
  OrganizationIdResponse,
  NotFound => PackageNotFound
}
import com.pennsieve.streaming.query.{ QuerySequencer, WsClient }
import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.TimeSeriesFlow.{ SessionFilters, SessionMontage }
import com.pennsieve.streaming.{ RangeLookUp, UnitRangeLookUp }
import com.typesafe.config.Config
import scalikejdbc.DBSession
import uk.me.berndporr.iirj.Cascade

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.util.{ Failure, Success, Try }

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

  def timeseriesQuery(
    claim: Claim,
    packageOrgId: Option[Int] = None
  )(
    packageId: String,
    startAtEpochParam: Option[String]
  ): Route = {
    val flow = ports.getChannels(packageId, claim, packageOrgId).map {
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

  val validateMontage: Claim => Option[Int] => String => Route =
    claim => new MontageValidationService(claim).route _

  def claimToRoutes(claim: Claim): Route = {
    concat(
      claimToDiscoverRoutes(claim),
      pathPrefix("ts") {
        concat(path("query") {
          parameter('package, 'startAtEpoch ?)(timeseriesQuery(claim))
        }, pathPrefix("retrieve") {
          retrieveRoutes(claim)
        }, path("validate-montage") {
          parameter('package)(validateMontage(claim)(None))
        })
      }
    )
  }

  private def unexpectedError(unexpected: Throwable, packageId: String): Route = {
    val message = s"error looking up organization id for package ($packageId): $unexpected"
    log.noContext.error(message)
    val error =
      TimeSeriesException.UnexpectedError(message)
    complete {
      error.statusCode -> error
    }
  }

  private def noOrgId(result: Try[OrganizationIdResponse], packageId: String): Route = {
    result match {
      case Success(PackageNotFound()) => complete(NotFound -> s"$packageId not found")
      case Success(NotTimeSeries()) =>
        complete(BadRequest -> s"package $packageId is not a $TimeSeries")
      case Success(Error(unexpected)) => unexpectedError(unexpected, packageId)
      case Failure(unexpected) => unexpectedError(unexpected, packageId)
      //Only the Success(Id) case is missing and that should be handled by the caller
      case unexpectedCase =>
        throw new AssertionError(
          s"Programming error: unexpected case: $unexpectedCase while looking up organization id for package $packageId"
        )
    }
  }

  def discoverQueryRoute(claim: Claim): Route =
    path("query") {
      parameter('package, 'startAtEpoch ?) { (packageId, startAtEpoch) =>
        onComplete(ports.discoverApiClient.getOrganizationId(packageId)) {
          case Success(Id(orgId)) =>
            timeseriesQuery(claim, Some(orgId))(packageId, startAtEpoch)
          case result: Try[OrganizationIdResponse] => noOrgId(result, packageId)
        }
      }
    }

  def retrieveRoutes(claim: Claim): Route = {
    concat(continuousQuery(claim), unitQuery(claim), concat(segmentQuery))
  }

  def discoverValidateMontageRoute(claim: Claim): Route =
    path("validate-montage") {
      parameter('package) { packageId =>
        onComplete(ports.discoverApiClient.getOrganizationId(packageId)) {
          case Success(Id(packageOrgId)) =>
            validateMontage(claim)(Some(packageOrgId))(packageId)
          case result: Try[OrganizationIdResponse] => noOrgId(result, packageId)
        }
      }
    }

  def claimToDiscoverRoutes(claim: Claim): Route = {
    pathPrefix("discover" / "ts") {
      concat(
        discoverQueryRoute(claim),
        pathPrefix("retrieve")(retrieveRoutes(claim)),
        discoverValidateMontageRoute(claim)
      )
    }
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
