/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.blackfynn.streaming.query

import akka.Done
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.{ ActorMaterializer, KillSwitches }
import cats.data.EitherT
import cats.implicits._
import com.blackfynn.domain.Error
import com.blackfynn.service.utilities.ContextLogger
import com.blackfynn.streaming.query.TimeSeriesQueryUtils.trim
import com.blackfynn.streaming.{ RangeLookUp, UnitRangeLookUp }
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
  materializer: ActorMaterializer,
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
