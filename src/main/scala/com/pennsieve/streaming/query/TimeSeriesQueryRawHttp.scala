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
import com.pennsieve.streaming.server.FilterStateTracker
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
    filters: mutable.Map[String, FilterStateTracker],
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
    filters: mutable.Map[String, FilterStateTracker],
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

            log.noContext.info(
              s"Getting  ${math.abs(butterworth.getLatestTimestamp - tstart)} -- ${(100 / leadChannel.lookUp.sampleRate) * 1e6}"
            )

            // If the start time of the requested block is not consecutive then reset filter.
            // difference between last end and current start > 100 samples
            if (math.abs(butterworth.getLatestTimestamp - tstart) > (100 / leadChannel.lookUp.sampleRate) * 1e6) {
              log.noContext.info(
                s"Resetting filter ${math.abs(butterworth.getLatestTimestamp - tstart)} -- ${(100 / leadChannel.lookUp.sampleRate) * 1e6}"
              )
              butterworth.reset()
            }

            // Update the filter to track latest timestamp.
            butterworth.setLatestTimestamp(tend)

            // Calculate padLength in case of reset filter
            val padLength = getFilterTransientLength(
              butterworth.getFilterOrder,
              butterworth.getMaxFilterFreq,
              leadChannel.lookUp.sampleRate
            )

            // Apply filters
            applyFilterWithPadding(trimmed, butterworth, padLength)

          }
          case None => Future.successful(trimmed)
        }

        // take up to the given limit
        limited <- filtered.flatMap(_.runWith(Sink.seq)).map { seq =>
          val limitedSeq = limit.map(l => seq.take(l)).getOrElse(seq)
          Source(limitedSeq)
        }

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

  private def getFilterTransientLength(
    order: Int,
    cutoffFreq: Double,
    samplingRate: Double
  ): Int = {
    // Simple conservative estimate
    // Rule: 5-10 cycles of the cutoff frequency, scaled by filter order

    val cyclesAtCutoff = samplingRate / cutoffFreq
    val conservativeCycles = 8.0 // 8 cycles is usually safe
    val orderFactor = 1.0 + (order - 1) * 0.5 // Linear scaling with order

    log.noContext.info(
      s"FilterInfo: sf:${samplingRate} - cutoff:${cutoffFreq} - cyclesAtCutoff${cyclesAtCutoff} - consCycles:${conservativeCycles} - orderFactor:${orderFactor} ---> total:${cyclesAtCutoff * conservativeCycles * orderFactor}"
    )

    (cyclesAtCutoff * conservativeCycles * orderFactor).ceil.toInt
  }

  private def applyFilterWithPadding(
    data: Source[Double, Any],
    filter: FilterStateTracker,
    padLength: Int
  ): Future[Source[Double, Any]] = {

    // Reset filter to clear any previous state
    if (filter.isClean) {
      log.noContext.info(s"Filter in init state: adding padding with length: $padLength")
      // Materialize the data to work with it
      data.runWith(Sink.seq).map { dataSeq =>
        val dataVector = dataSeq.toVector

        if (dataVector.nonEmpty) {
          // Create reflected prewarm vector
          val prewarmVector = createReflectedPrewarmVector(dataVector, padLength)

          // Apply prewarming (process but don't emit these values)
          prewarmVector.foreach(filter.filter)

          // Now process actual data with warmed-up filter
          val processedData = dataVector.map(filter.filter)
          Source(processedData)
        } else {
          Source.empty[Double]
        }
      }
    } else {
      // Filter already warmed up, just apply filtering
      Future.successful(data.map(filter.filter))
    }
  }

  /**
    * Create reflected prewarm vector from the beginning of the original signal
    * Simple approach: take the first N samples and reverse them
    */
  private def createReflectedPrewarmVector(
    originalData: Vector[Double],
    requiredLength: Int
  ): Vector[Double] = {

    if (originalData.isEmpty) {
      // If no data, return zeros
      return Vector.fill(requiredLength)(0.0)
    }

    if (originalData.length == 1) {
      // Single value: repeat that value
      return Vector.fill(requiredLength)(originalData.head)
    }

    if (originalData.length >= requiredLength) {
      // Sufficient data: take first 'requiredLength' samples and reverse them
      originalData.take(requiredLength).reverse
    } else {
      // Insufficient data: reflect what we have and pad as needed
      createReflectedWithPadding(originalData, requiredLength)
    }
  }

  /**
    * Handle case where original data is shorter than required warmup length
    */
  private def createReflectedWithPadding(
    originalData: Vector[Double],
    requiredLength: Int
  ): Vector[Double] = {

    val dataLength = originalData.length

    if (dataLength >= requiredLength / 2) {
      // We have at least half the required length
      // Reflect the data and take what we need
      val reflected = originalData.reverse
      val combined = reflected ++ originalData

      if (combined.length >= requiredLength) {
        combined.take(requiredLength)
      } else {
        // Still not enough, pad with edge values
        val padding = Vector.fill(requiredLength - combined.length)(originalData.head)
        padding ++ combined.take(requiredLength - padding.length)
      }
    } else {
      // Very short data: repeat the reflection pattern
      val reflected = originalData.reverse
      val pattern = reflected ++ originalData

      // Repeat the pattern until we have enough samples
      val repeated = Iterator.continually(pattern).flatten.take(requiredLength).toVector
      repeated
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
