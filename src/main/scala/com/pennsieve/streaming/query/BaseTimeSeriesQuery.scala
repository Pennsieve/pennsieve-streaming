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

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.TimeSeriesQueryUtils._
import com.pennsieve.streaming.server.Montage
import com.pennsieve.streaming.{ LookupResultRow, Segment, TimeSeriesMessage, UnitRangeEntry }

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Created by jsnavely on 3/7/17.
  */
case class RangeRequest(
  channelNodeId: String,
  channelName: String,
  start: Long,
  end: Long,
  limit: Option[Int] = None,
  pixelWidth: Long,
  lookUp: LookupResultRow,
  totalRequests: Option[Long] = None,
  sequenceId: Option[Long] = None
)

case class UnitRangeRequest(
  channel: String,
  start: Long,
  end: Long,
  limit: Option[Long] = None,
  pixelWidth: Long,
  spikeDuration: Long,
  spikeDataPointCount: Int,
  sampleRate: Double,
  lookUp: UnitRangeEntry,
  totalRequests: Option[Long] = None,
  sequenceId: Option[Long] = None
)

object BaseTimeSeriesQuery {
  def performStreamResampling(
    resample: Boolean,
    durationMicroseconds: Long,
    requestedSampleTimePeriodMicroseconds: Long,
    segmentSampleRate: Double,
    results: Source[Double, Any]
  )(implicit
    ec: ExecutionContext,
    system: ActorSystem
  ): Future[(Seq[Double], Double)] = {

    if (resample) {
      val requestedChunkSizeSamples
        : Double = requestedSampleTimePeriodMicroseconds.toDouble * segmentSampleRate / 1e6 //number of resampled datapoints per time period

      val actualChunkSizeSamples: Int =
        math.round(requestedChunkSizeSamples).toInt

      val actualChunkSizeTime: Double = actualChunkSizeSamples / segmentSampleRate * 1e6

      val totalDataPoints: Double = (durationMicroseconds / 1e6) * segmentSampleRate

      val numberOfChunks: Int =
        math.floor(totalDataPoints / actualChunkSizeSamples).toInt

      results
        .grouped(actualChunkSizeSamples)
        .take(numberOfChunks)
        .collect({ case x if x.length > 0 => findMinMaxUnsafe(x) })
        .mapConcat(p => List(p._1, p._2))
        .runWith(Sink.seq)
        .map(s => (s, actualChunkSizeTime))
    } else {
      results.runWith(Sink.seq).map { data =>
        (data, requestedSampleTimePeriodMicroseconds.toDouble)
      }
    }

  }

}
class BaseTimeSeriesQuery(
)(implicit
  ec: ExecutionContext,
  log: ContextLogger,
  system: ActorSystem
) {

  def time[R](name: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    log.noContext.info(s"task $name Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def buildTimeSeriesMessage(
    leadChannel: RangeRequest,
    secondaryChannel: Option[RangeRequest],
    results: Source[Double, Any],
    startTime: Long,
    endTime: Long,
    resampled: Boolean
  ): Future[TimeSeriesMessage] = {

    val channelName =
      Montage.getMontageName(leadChannel.channelName, secondaryChannel.map(_.channelName))

    val pixelWidth = leadChannel.pixelWidth
    val segmentSampleRate = leadChannel.lookUp.sampleRate
    val totalRequests = leadChannel.totalRequests
    val sequenceId = leadChannel.sequenceId

    val pageStart = leadChannel.start
    val pageEnd = leadChannel.end

    val requestedSampleTimePeriodMicroseconds = if (resampled) {
      pixelWidth //number of microseconds per value
    } else {
      math.round(1e6 / segmentSampleRate) //rate is in hertz, so we need to divide by 1e6
    }

    val durationMicroseconds = endTime - startTime

    for {
      (data, actualSamplePeriod) <- BaseTimeSeriesQuery.performStreamResampling(
        resampled,
        durationMicroseconds,
        requestedSampleTimePeriodMicroseconds,
        segmentSampleRate,
        results
      )
    } yield {
      val seg = Segment(
        startTs = startTime, ///should be the time of the first sample (may be different from the first requested time)
        source = leadChannel.channelNodeId,
        unit = "V",
        samplePeriod = actualSamplePeriod,
        requestedSamplePeriod = pixelWidth,
        pageStart = pageStart, //the start time originally requested
        pageEnd = pageEnd,
        isMinMax = resampled,
        unitM = 1000,
        segmentType = "Continuous",
        nrPoints = data.size,
        data = data,
        channelName = channelName
      )

      TimeSeriesMessage(segment = Some(seg)).update(
        _.totalResponses := totalRequests getOrElse 1,
        _.responseSequenceId := sequenceId getOrElse 0
      )
    }

  }

}
