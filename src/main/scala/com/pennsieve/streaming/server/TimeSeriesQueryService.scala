package com.pennsieve.streaming.server

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{ Flow, Sink }
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.WsClient
import com.pennsieve.streaming.server.TimeSeriesFlow.{ SessionFilters, SessionMontage }
import com.typesafe.config.Config
import scalikejdbc.DBSession
import uk.me.berndporr.iirj.Cascade
import cats.instances.future._
import com.pennsieve.streaming.{ RangeLookUp, UnitRangeLookUp }

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.ExecutionContext

class TimeSeriesQueryService(
  sessionFilters: SessionFilters,
  sessionMontages: SessionMontage,
  rangeLookUp: RangeLookUp,
  unitRangeLookUp: UnitRangeLookUp,
  connectionCounter: AtomicLong
)(implicit
  log: ContextLogger,
  ports: WebServerPorts,
  system: ActorSystem,
  wsClient: WsClient,
  config: Config,
  dbSession: DBSession,
  ec: ExecutionContext
) {

  val jwtConfig: Jwt.Config = getJwtConfig(config)

  def route(
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

        val session = Jwt.generateToken(claim)(jwtConfig).value
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

}
