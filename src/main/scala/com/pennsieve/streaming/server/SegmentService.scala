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

import com.pennsieve.domain.Error
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.findContiguousTimeSpans
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.pennsieve.streaming.RangeLookUp

import cats.implicits._
import scala.util.Try

class SegmentService(rangeLookUp: RangeLookUp, defaultGapThreshold: Double)
    extends Directives
    with TSJsonSupport {

  def getGapThreshold(threshold: Option[String], default: Double): Either[Error, Double] =
    Try(threshold.map(_.toDouble)).toEither
      .leftMap(t => Error(s"problem parsing double: ${t.getMessage}"))
      .map(_.getOrElse(default))

  def route = path("segments") {
    parameter('start.?, 'end.?, 'channel, 'gapThreshold.?) { (start, end, channel, gapThreshold) =>
      val lookups = for {
        startString <- start
        endString <- end
        startL <- Try(startString.toLong).toOption
        endL <- Try(endString.toLong).toOption
      } yield rangeLookUp.lookup(startL, endL, channel)

      val result: Either[Error, List[(Long, Long)]] =
        getGapThreshold(gapThreshold, defaultGapThreshold).map(
          findContiguousTimeSpans(lookups.fold(rangeLookUp.get(channel))(l => l), _)
        )

      result match {
        case Left(e) => complete(StatusCodes.BadRequest -> e.getMessage())
        case Right(spans) =>
          complete {
            spans
          }
      }
    }
  }
}
