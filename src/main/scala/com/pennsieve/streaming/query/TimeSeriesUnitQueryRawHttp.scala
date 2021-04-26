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
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.trimToRange
import com.pennsieve.streaming.{ Event, TimeSeriesMessage }
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }

/**
  * Created by jsnavely on 1/12/17.
  */
class TimeSeriesUnitQueryRawHttp(
  config: Config,
  wsClient: WsClient
)(implicit
  ec: ExecutionContext,
  log: ContextLogger,
  system: ActorSystem
) extends BaseTimeSeriesQuery {

  /***********
    *
    *   channel  : the identifier for the electrode
    *   start    : timestamp in microseconds UTC
    *   end      : timestamp in microseconds UTC
    *   limit    : the maximum number of data points to return
    *   timeStep : the number of microseconds between datapoints.
    *
   **********/
  val s3_base = config.getString("timeseries.s3-base-url")
  val sendSpikeThreshold = config.getInt("timeseries.send-spike-threshold")

  def rangeQuery(r: UnitRangeRequest): Future[TimeSeriesMessage] = {
    r.limit match {
      case None => rangeQueryUnlimited(r)
      case Some(0) =>
        Future(
          TimeSeriesMessage(
            event = Some(
              Event(
                source = r.channel,
                pageStart = r.start,
                pageEnd = r.end,
                samplePeriod = r.pixelWidth
              )
            )
          )
        )
      case Some(limit) => rangeQueryWithLimit(r, limit.toInt)
    }
  }

  def rangeQueryNow(rr: UnitRangeRequest): TimeSeriesMessage = {
    Await.result(rangeQuery(rr), 15.seconds)
  }

  def shouldSendSpikeData(pixelWidth: Long, spikeDuration: Long): Boolean = {
    (pixelWidth * sendSpikeThreshold) < spikeDuration
  }

  def getSpikes(r: UnitRangeRequest): Future[Source[Seq[(Double, Double)], Any]] = {
    if (shouldSendSpikeData(r.pixelWidth, r.spikeDuration)) {
      wsClient.getSpikes(r.lookUp.tsblob, r.pixelWidth, r.spikeDataPointCount, r.spikeDuration)
    } else {
      Future.successful(Source.empty)
    }
  }

  def buildEvents(
    times: Seq[(Long, Long)],
    spikes: Iterable[Seq[(Double, Double)]],
    range: (Long, Long),
    r: UnitRangeRequest
  ): TimeSeriesMessage = {

    val flatTimes = times.flatMap(e => List(e._1, e._2))

    val trimmedSpikes: Iterable[Seq[(Double, Double)]] =
      trimToRange(range, spikes)

    val flatSpikes =
      trimmedSpikes.flatten.flatMap(sp => List(sp._1, sp._2)).toList

    val events = Event(
      source = r.channel,
      pageStart = r.start,
      pageEnd = r.end,
      samplePeriod = r.pixelWidth,
      pointsPerEvent = r.spikeDataPointCount,
      times = flatTimes,
      data = flatSpikes
    )

    TimeSeriesMessage(event = Some(events)).update(
      _.totalResponses := r.totalRequests getOrElse 1,
      _.responseSequenceId := r.sequenceId getOrElse 0
    )
  }

  def buildEventsFromSources(
    events: Source[EventSummary, Any],
    spikes: Source[Seq[(Double, Double)], Any],
    r: UnitRangeRequest
  ): Future[TimeSeriesMessage] = {

    for {
      _events <- events.runWith(Sink.seq)
      _spikes <- spikes.runWith(Sink.seq)
    } yield {

      val _range = for {
        start <- _events.headOption
        end <- _events.lastOption
      } yield (start.minIndex, end.maxIndex)

      val range: (Long, Long) = _range getOrElse (0, 0)
      val times = _events.map(e => (e.avgTime, e.count))

      buildEvents(times, _spikes, range, r)
    }
  }

  def rangeQueryUnlimited(r: UnitRangeRequest): Future[TimeSeriesMessage] = {
    time(s"rangeQueryUnlimited: ${r.channel} start: ${r.start}, end: ${r.end}") {
      for {
        times <- wsClient.getEventsSummary(r.lookUp.tsindex, r.pixelWidth, r.start, r.end)
        spikes <- getSpikes(r)
        msg <- buildEventsFromSources(times, spikes, r)
      } yield msg
    }
  }

  def rangeQueryWithLimit(r: UnitRangeRequest, limit: Int): Future[TimeSeriesMessage] = {
    time(s"rangeQueryWithLimit: ${r.channel} start: ${r.start}, end: ${r.end}") {
      for {
        times <- wsClient.getEventsSummary(r.lookUp.tsindex, r.pixelWidth, r.start, r.end)
        spikes <- getSpikes(r)
        msg <- buildEventsFromSources(times.take(limit), spikes.take(limit), r)
      } yield msg
    }
  }

}
