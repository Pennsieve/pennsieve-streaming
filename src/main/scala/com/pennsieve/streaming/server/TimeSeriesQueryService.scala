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
  implicit
  log: ContextLogger,
  ports: WebServerPorts,
  system: ActorSystem,
  wsClient: WsClient,
  config: Config,
  dbSession: DBSession,
  ec: ExecutionContext
) {

  val jwtConfig: Jwt.Config = getJwtConfig(config)

  val connectionCounter: AtomicLong = new AtomicLong()

  def getConnectionCount: Long = connectionCounter.get()

  // A global map of session ids to filters that are active on
  // specific channels during that session
  val sessionFilters: SessionFilters =
    new ConcurrentHashMap[String, concurrent.Map[String, FilterStateTracker]]().asScala

  // A global map of session ids to the montages that are active on
  // specific packages during that session
  val sessionMontages: SessionMontage =
    new ConcurrentHashMap[String, concurrent.Map[String, MontageType]]().asScala

  val rangeLookUp =
    new RangeLookUp(ports.rangeLookupQuery, config.getString("timeseries.s3-base-url"))

  val unitRangeLookUp =
    new UnitRangeLookUp(ports.unitRangeLookupQuery, config.getString("timeseries.s3-base-url"))

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
        val channelFilters: concurrent.Map[String, FilterStateTracker] =
          new ConcurrentHashMap[String, FilterStateTracker]().asScala
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
