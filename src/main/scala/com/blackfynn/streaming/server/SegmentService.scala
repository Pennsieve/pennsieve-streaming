package com.blackfynn.streaming.server

import com.blackfynn.domain.Error
import com.blackfynn.streaming.query.TimeSeriesQueryUtils.findContiguousTimeSpans
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.blackfynn.streaming.RangeLookUp

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
    parameter('session, 'start.?, 'end.?, 'channel, 'gapThreshold.?) {
      (_, start, end, channel, gapThreshold) =>
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
