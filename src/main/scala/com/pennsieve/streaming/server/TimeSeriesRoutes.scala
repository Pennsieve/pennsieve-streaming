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
import akka.http.scaladsl.server.Directives.{ _symbol2NR, concat, parameter, path, pathPrefix }
import akka.http.scaladsl.server.Route
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.{ RangeLookUp, UnitRangeLookUp }
import com.pennsieve.streaming.query.{ QuerySequencer, WsClient }
import com.typesafe.config.Config
import scalikejdbc.DBSession

import scala.concurrent.ExecutionContext

class TimeSeriesRoutes(
  implicit
  log: ContextLogger,
  ports: WebServerPorts,
  system: ActorSystem,
  wsClient: WsClient,
  config: Config,
  dbSession: DBSession,
  ec: ExecutionContext
) {

  val queryLimit = config.getLong("timeseries.query-limit")

  val defaultGapThreshold = config.getDouble("timeseries.default-gap-threshold")

  val rangeLookUp =
    new RangeLookUp(ports.rangeLookupQuery, config.getString("timeseries.s3-base-url"))

  val unitRangeLookUp =
    new UnitRangeLookUp(ports.unitRangeLookupQuery, config.getString("timeseries.s3-base-url"))
  val querySequencer =
    new QuerySequencer(rangeLookUp, unitRangeLookUp)

  type ClaimToRoute = Claim => Route

  val segmentQuery: Route =
    new SegmentService(rangeLookUp, defaultGapThreshold).route
  val continuousQuery: ClaimToRoute =
    claim => new ContinuousQueryService(querySequencer, queryLimit, claim).route
  val unitQuery: ClaimToRoute =
    claim => new UnitQueryService(querySequencer, queryLimit, claim).route

  val validateMontage: MontageValidationService = new MontageValidationService()

  def timeseriesQuery(
    claim: Claim,
    getChannelsQuery: GetChannelsQuery
  )(
    packageId: String,
    startAtEpoch: Option[String]
  ): Route = ???

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

  def claimToTimeSeriesRoutes(claim: Claim, getChannelsQuery: GetChannelsQuery): Route = {
    concat(
      queryRoute(claim, getChannelsQuery),
      pathPrefix("retrieve")(retrieveRoutes(claim)),
      validateMontageRoute(claim, getChannelsQuery)
    )
  }
}
