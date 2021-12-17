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

import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.scaladsl.Source
import cats.implicits._
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.{ overLimit, parseLong }
import de.knutwalker.akka.http.support.CirceHttpSupport._
import io.circe.Encoder

import scala.util.Success
import scala.util.Failure
import com.pennsieve.streaming.query.{ QuerySequencer, UnitRangeRequest }
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT
import com.pennsieve.streaming.UnitRangeEntry

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

class UnitQueryService(
  querySequencer: QuerySequencer,
  queryLimit: Long,
  claim: Claim,
  packageOrgId: Option[Int]
)(implicit
  ports: WebServerPorts,
  ec: ExecutionContext
) extends Directives
    with TSJsonSupport {

  implicit val jsonStreamingSupport =
    EntityStreamingSupport.json()
  implicitly[Encoder[Long]]

  def route: Route = path("unit") {
    parameter('start, 'end, 'channel, 'limit) { (start, end, channelNodeId, limit) =>
      {
        get {
          val unitQuery: WithErrorT[Source[Long, Any]] =
            for {
              channelAndLogContext <- {
                ports.getChannelByNodeId(channelNodeId, claim, packageOrgId)
              }
              (channel, logContext) = channelAndLogContext

              startL <- parseLong(start)
                .leftMap(TimeSeriesException.fromCoreError)
              endL <- parseLong(end)
                .leftMap(TimeSeriesException.fromCoreError)
              _ <- overLimit(startL, endL, channel.rate, queryLimit)
                .toEitherT[Future]
                .leftMap(TimeSeriesException.fromCoreError)
              limitL = Try(limit.toLong).toOption
              emptyLookup = UnitRangeEntry(
                0,
                min = 0,
                max = 0,
                channel = "",
                count = 0,
                tsindex = "",
                tsblob = ""
              )

              ur = UnitRangeRequest(
                channel.nodeId,
                startL,
                endL,
                limitL,
                pixelWidth = 0,
                spikeDuration = 0,
                spikeDataPointCount = 0,
                sampleRate = 0.0,
                lookUp = emptyLookup,
                totalRequests = None,
                sequenceId = None
              )

              qresult <- querySequencer
                .unitRangeRequestT(ur)
                .leftMap(TimeSeriesException.fromCoreError)

            } yield qresult

          onComplete(unitQuery.value) {
            case Success(either) =>
              either.fold(e => complete { e.statusCode -> e }, data => complete { data })
            case Failure(unexpected) => {
              unexpected.printStackTrace()
              val error =
                TimeSeriesException.UnexpectedError(unexpected.toString)
              complete {
                error.statusCode -> error
              }
            }
          }
        }
      }
    }
  }

}
