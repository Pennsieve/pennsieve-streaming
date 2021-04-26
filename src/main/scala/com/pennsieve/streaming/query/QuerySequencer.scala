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

package com.pennsieve.streaming.query

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.KillSwitches
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.domain.Error
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.trim
import com.pennsieve.streaming.{ RangeLookUp, UnitRangeLookUp }
import scalikejdbc.DBSession

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Created by jsnavely on 3/9/17.
  */
class QuerySequencer(
  rangeLookUp: RangeLookUp,
  unitRangeLookup: UnitRangeLookUp
)(implicit
  ec: ExecutionContext,
  log: ContextLogger,
  system: ActorSystem,
  dbSession: DBSession,
  val wsClient: WsClient
) extends BaseTimeSeriesQuery {

  def unitRangeRequestS3(ur: UnitRangeRequest): Source[Long, Any] =
    Source(unitRangeLookup.lookup(ur.start, ur.end, ur.channel))
      .mapAsync(1) { lookupresult =>
        wsClient.trimEvents(lookupresult.tsindex, ur.start, ur.end)
      }
      .flatMapConcat(s => s)

  def unitRangeRequestT(ur: UnitRangeRequest): EitherT[Future, Error, Source[Long, Any]] = {
    if (ur.channel.length > 0) {
      EitherT.rightT(unitRangeRequestS3(ur))
    } else {
      EitherT.leftT(Error("bad request"))
    }
  }

  def continuousRangeRequestS3[A](
    rr: RangeRequest,
    drain: (Source[(Long, Double), Any]) => Future[Done] = _.runWith(Sink.ignore)
  ): Source[(Long, Double), Any] = {
    val killswitch = KillSwitches.shared("continuous")

    Source(rangeLookUp.lookup(rr.start, rr.end, rr.channelNodeId))
      .via(killswitch.flow)
      .mapAsync(1) { lookupresult => //if we find hits in the range table
        for {
          dp <- wsClient
            .getDataSource(lookupresult.file)
          (start, end, dataPoints) = trim[Double](
            lookupresult.min,
            lookupresult.max,
            rr.start,
            rr.end,
            lookupresult.sampleRate,
            dp
          )
          period = Math.round(1e6 / lookupresult.sampleRate)
          timeValuePairs: Source[(Long, Double), Any] = dataPoints.zipWithIndex
            .map {
              case (value, index) => (start + (index * period), value)
            }
        } yield timeValuePairs
      }

      // add a drain to drain sources if the downstream stops
      // requesting them
      .via(new Drain(killswitch, drain))
      .flatMapConcat(s => s)
  }

  def continuousRangeRequestT[A](
    rr: RangeRequest,
    drain: (Source[(Long, Double), Any]) => Future[Done] = _.runWith(Sink.ignore)
  ): EitherT[Future, Error, Source[(Long, Double), Any]] = {
    if (rr.channelNodeId.length > 0) {
      EitherT.rightT(continuousRangeRequestS3(rr, drain))
    } else {
      EitherT.leftT(Error("bad request"))
    }
  }

}
