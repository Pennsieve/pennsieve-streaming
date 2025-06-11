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
import com.pennsieve.streaming.{ Segment, TimeSeriesMessage }
import uk.me.berndporr.iirj.Cascade

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

/** Provides functions for transforming RangeRequest queries into
  * streams of timeseries data
  */
class TimeSeriesQueryRawHttp(
  wsClient: WsClient
)(implicit
  ec: ExecutionContext,
  log: ContextLogger,
  system: ActorSystem
) extends BaseTimeSeriesQuery {

  /** A query on one (or optionally two) channels. If two channels are
    * provided, they will be montaged into a single datasteam. The
    * given filters map will be used to filter the resulting stream.
    *
    * @param leadChannel A channel representing timeseries data
    * @param filters A map of channels to the filter that should be
    *        applied to each channel. This map will be used to find the
    *        correct filter to apply on this channel.
    * @param secondaryChannel An optional secondary channel to montage
    *        against the lead channel
    *
    * @return A future that will resolve with a TimeseriesMessage that can
    *         be sent over a websocket connection
    */
  def rangeQuery(
    leadChannel: RangeRequest,
    filters: mutable.Map[String, Cascade],
    secondaryChannel: Option[RangeRequest] = None
  ): Future[TimeSeriesMessage] = {
    leadChannel.limit match {
      case Some(0) =>
        // no data requested, return an "empty" timeseries message
        Future(
          TimeSeriesMessage(
            segment = Some(
              Segment(
                startTs = leadChannel.start,
                pageStart = leadChannel.start,
                pageEnd = leadChannel.end,
                source = leadChannel.channelNodeId,
                data = Seq(),
                requestedSamplePeriod = leadChannel.pixelWidth,
                segmentType = "Continuous",
                channelName = Montage
                  .getMontageName(leadChannel.channelName, secondaryChannel.map(_.channelName))
              )
            ),
            responseSequenceId = 0,
            totalResponses = 1
          )
        )
      case _ =>
        fetchDataStream(leadChannel, leadChannel.limit, filters, secondaryChannel)
    }
  }

  /** Get a stream of timeseries data from S3, applying all necessary
    * transformations along the way:
    *
    *   Ch A -----
    *             |
    *             V
    *       | Montaging | ------> | Filtering | ----> | Resampling |
    *             ^
    *             |
    *   Ch B -----
    *
    * @param leadChannel A channel representing timeseries data
    * @param limit An optional cap on the maximum amount of datapoints
    *        to retrieve
    * @param filters A map of channels to the filter that should be
    *        applied to each channel. This map will be used to find
    *        the correct filter to apply on this channel.
    * @param secondaryChannel: An optional secondary channel to
    *        montage against the lead channel
    *
    * @return A future that will resolve with a TimeseriesMessage that
    *         can be sent over a websocket connection
    */
  private def fetchDataStream(
    leadChannel: RangeRequest,
    limit: Option[Int],
    filters: mutable.Map[String, Cascade],
    secondaryChannel: Option[RangeRequest]
  ): Future[TimeSeriesMessage] = {
    time(
      s"fetchDataStream: ${leadChannel.channelName} "
        + s"start: ${leadChannel.start}, end: ${leadChannel.end}"
    ) {
      val resampled =
        shouldResample(leadChannel.lookUp.sampleRate, leadChannel.pixelWidth)
      val montageName =
        Montage.getMontageName(leadChannel.channelName, secondaryChannel.map(_.channelName))
      val filter = filters.get(leadChannel.channelNodeId);

      for {
        // request data
        leadChannelData <- requestData(leadChannel.lookUp.file)
        secondaryChannelData <- secondaryChannel match {
          case Some(rangeRequest) =>
            requestData(rangeRequest.lookUp.file).map(Some.apply)
          case None => Future.successful(None)
        }

        // montage the two channels
        montaged = montage(leadChannelData, secondaryChannelData)

        // trim resulting datastream
        (tstart, tend, trimmed) = trim(
          leadChannel.lookUp.min,
          leadChannel.lookUp.max,
          leadChannel.start,
          leadChannel.end,
          leadChannel.lookUp.sampleRate,
          montaged
        )

        // apply any filters
        filtered = filter match {
          case Some(butterworth) => {
            filters.put(montageName, butterworth)
            applyFilterWithPadding(trimmed, butterworth)
          }
          case None => trimmed
        }

        // take up to the given limit
        limited = limit.map(l => filtered.take(l)).getOrElse(filtered)

        // return a timeseries message
        tm <- buildTimeSeriesMessage(
          leadChannel,
          secondaryChannel,
          limited,
          tstart,
          tend,
          resampled
        )
      } yield tm
    }
  }

  private def applyFilterWithPadding(
    data: Source[Double, Any],
    filter: Cascade
  ): Source[Double, Any] = {
    val padLength = 50

    // Reset filter to clear any previous state
    // TODO: improve by only padding and resetting when filter does not have an existing state.
    filter.reset()

    data.prefixAndTail(1).flatMapConcat {
      case (firstElement, restOfStream) =>
        if (firstElement.nonEmpty) {
          val initialValue = firstElement.head

          // Create padding source
          val padding = Source.repeat(initialValue).take(padLength)

          // Process padding through filter (to warm it up) but don't emit
          val warmUpFilter = padding.map(filter.filter).runWith(Sink.ignore)

          // Now process the actual data
          Source.future(warmUpFilter).flatMapConcat { _ =>
            Source(firstElement).concat(restOfStream).map(filter.filter)
          }
        } else {
          Source.empty[Double]
        }
    }
  }

  /** Given a corresponding pair of data streams, subtract
    * secondaryChannel's data from leadChannel. If no secondaryChannel
    * is provided, nothing will be subtracted from leadChannel.
    *
    * @param leadChannel A channel representing timeseries data
    * @param secondaryChannel An optional secondary channel to montage
    *        against the lead channel
    *
    * @return A future that will resolve with a tuple containing the
    *         montaged data and a list of the given closeables
    */
  private def montage(
    leadChannel: Source[Double, Any],
    secondaryChannel: Option[Source[Double, Any]]
  ): Source[Double, Any] =
    secondaryChannel match {
      case Some(data) =>
        leadChannel.zip(data).map(p => p._1 - p._2)
      case None => leadChannel
    }

  /** Given a location, stream the data from that location with the wsClient.
    *
    * @param location The location of the requested data
    *
    * @return A future that will resolve with a tuple containing a
    *         stream of the requested data and a closeable object that
    *         should be used along with the wsClient.close method to
    *         release all resources
    */
  private def requestData(location: String): Future[Source[Double, Any]] =
    wsClient.getDataSource(location)
}
