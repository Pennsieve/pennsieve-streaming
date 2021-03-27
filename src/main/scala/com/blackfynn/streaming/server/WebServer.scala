// Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.streaming.server

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{ BadRequest, Unauthorized }
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Sink }
import cats.instances.future._
import com.blackfynn.auth.middleware.Jwt.{ Claim, Token }
import com.blackfynn.auth.middleware.{ Jwt, ServiceClaim }
import com.blackfynn.service.utilities.ContextLogger
import com.blackfynn.streaming.query.{ QuerySequencer, WsClient }
import com.blackfynn.streaming.server.TSJsonSupport._
import com.blackfynn.streaming.server.TimeSeriesFlow.{ SessionFilters, SessionMontage }
import com.blackfynn.streaming.{ RangeLookUp, UnitRangeLookUp }
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
  implicit val materializer = ActorMaterializer()

  val connectionCounter = new AtomicLong()
  val startupTime = System.currentTimeMillis()

  val querySequencer =
    new QuerySequencer(rangeLookUp, unitRangeLookUp)

  def timeseriesQuery(
    maybeClaim: Option[Claim]
  )(
    session: String,
    packageId: String,
    startAtEpochParam: Option[String]
  ): Route = {
    val flow = ports.getChannels(session, packageId, maybeClaim).map {
      case (channels, logContext) =>
        val cmap = channels.map { c =>
          c.nodeId -> c
        }.toMap

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

  type MaybeClaimToRoute = Option[Claim] => Route

  val segmentQuery: Route =
    new SegmentService(rangeLookUp, defaultGapThreshold).route
  val continuousQuery: MaybeClaimToRoute =
    claim => new ContinuousQueryService(querySequencer, queryLimit, claim).route
  val unitQuery: MaybeClaimToRoute =
    claim => new UnitQueryService(querySequencer, queryLimit, claim).route

  val validateMontage: Option[Claim] => (String, String) => Route =
    claim => new MontageValidationService(claim).route _

  def maybeClaimToRoutes(claim: Option[Claim]): Route =
    pathPrefix("ts") {
      path("query") {
        parameter('session, 'package, 'startAtEpoch ?)(timeseriesQuery(claim))
      } ~ pathPrefix("retrieve") {
        continuousQuery(claim) ~ unitQuery(claim) ~ segmentQuery
      } ~ pathPrefix("health") {
        healthCheck
      } ~ pathPrefix("validate-montage") {
        parameter('session, 'package)(validateMontage(claim))
      }
    }

  def route: Route =
    extractCredentials {
      case Some(OAuth2BearerToken(token)) =>
        Jwt.parseClaim(Token(token)) match {
          case Right(Claim(ServiceClaim(_), _, _)) =>
            complete(HttpResponse(Unauthorized))

          case Right(claim) => maybeClaimToRoutes(Some(claim))

          case Left(_) => complete(HttpResponse(BadRequest))
        }

      case _ => maybeClaimToRoutes(None)
    }
}
