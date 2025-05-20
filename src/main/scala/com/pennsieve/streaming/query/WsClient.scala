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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.stream.scaladsl.{ FileIO, Flow, Framing, Keep, Sink, Source }
import akka.stream.{ OverflowStrategy, QueueOfferResult }
import akka.util.ByteString
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.resample
import com.pennsieve.streaming.query.chunker.{ ByteStringChunker, PredicateStreamChunker }
import java.nio.file.Paths
import com.pennsieve.streaming.server.StreamUtils.OptionFilter
import com.pennsieve.streaming.util.{ getDouble, getLong }
import scala.concurrent.{ Future, _ }
import scala.util.{ Failure, Success }

/**
  * Created by jsnavely on 1/19/17.
  */
case class EventSummary(minTime: Long, maxTime: Long, minIndex: Long, maxIndex: Long, count: Long) {
  def add(time: Long, indx: Long) =
    EventSummary(
      minTime = Math.min(minTime, time),
      maxTime = Math.max(maxTime, time),
      minIndex = math.min(minIndex, indx),
      maxIndex = math.max(maxIndex, indx),
      count = count + 1
    )
  def avgTime: Long = (minTime + maxTime) / 2
}

/**
  * A trait for websocket clients
  */
trait WsClient {

  implicit val ec: ExecutionContext

  /*
   * Get a stream of timeseries data from a file URL
   */
  def getDataSource(url: String): Future[Source[Double, Any]]

  /*
   * Get a stream of event timestamps from a neural unit data time
   * index file url.
   *
   * Data in these files are represented as a flat
   * list of utc microsecond timestamps.
   */
  def getEventSource(url: String): Future[Source[Long, Any]]

  /*
   * Retrieve a stream of event timestamp data, filtering out any
   * datapoints outside the given range
   */
  def trimEvents(url: String, start: Long, end: Long): Future[Source[Long, Any]] =
    getEventSource(url).map(
      _.dropWhile(t => t < start)
        .takeWhile(t => t <= end)
    )

  /*
   * Identify spikes in data
   */
  def getSpikes(
    tsblobUrl: String,
    pixelWidth: Long,
    spikeDataPointCount: Int,
    spikeDuration: Long
  ): Future[Source[Seq[(Double, Double)], Any]] = {
    val newChunkCount = (spikeDuration / pixelWidth) toInt

    getDataSource(tsblobUrl).map(
      _.grouped(spikeDataPointCount)
        .map(g => resample(g.toVector, newChunkCount))
    )
  }

  /*
   * Fetch summary of data from neural unit data time index files
   */
  def getEventsSummary(
    url: String,
    pixelWidth: Long,
    start: Long,
    end: Long
  ): Future[Source[EventSummary, Any]] =
    trimEvents(url, start, end).map(_.via(summaryFlow(pixelWidth)))

  /*
   * Takes a stream of timestamps, chunks it into time periods and
   * then summarizes each chunk
   */
  private def summaryFlow(pixelWidth: Long): Flow[Long, EventSummary, NotUsed] =
    Flow[Long].zipWithIndex
      .via(new PredicateStreamChunker(longEnough(_, _, pixelWidth)))
      .map(summarize)
      .via(OptionFilter)

  /*
   *  Tells us when we have accumulated enough (or too many events)
   *  for a pixelWidth
   */
  private def longEnough(s: Seq[(Long, Long)], last: (Long, Long), pixelWidth: Long) = {
    val threshold = for {
      first <- s.headOption
    } yield last._1 - first._1 >= pixelWidth
    threshold getOrElse false
  }

  private def summarize(events: Vector[(Long, Long)]): Option[EventSummary] = {
    for {
      f <- events.headOption
      summary = EventSummary(minTime = f._1, maxTime = f._1, minIndex = f._2, maxIndex = f._2, 0)
    } yield
      events.foldLeft(summary) { (s, event) =>
        s.add(event._1, event._2)
      }
  }
}

/**
  * A websocket client that assumes all file URLs are s3 paths, and
  * will stream the requested data straight from s3.
  */
class S3WsClient(
  appconfig: com.typesafe.config.Config
)(implicit
  val ec: ExecutionContext,
  system: ActorSystem
) extends WsClient {

  private val s3host = appconfig.getString("timeseries.s3-host")
  private val s3Port = appconfig.getInt("timeseries.s3-port")
  private val QueueSize = appconfig.getInt("timeseries.request-queue-size")

  // This idea came initially from this blog post:
  // http://kazuhiro.github.io/scala/akka/akka-http/akka-streams/2016/01/31/connection-pooling-with-akka-http-and-source-queue.html
  private val poolClientFlow =
    Http().cachedHostConnectionPool[Promise[HttpResponse]](host = s3host, port = s3Port)

  private val queue =
    Source
      .queue[(HttpRequest, Promise[HttpResponse])](QueueSize, OverflowStrategy.dropHead)
      .via(poolClientFlow)
      .toMat(Sink.foreach({
        case ((Success(resp), p)) => p.success(resp)
        case ((Failure(e), p)) => p.failure(e)
      }))(Keep.left)
      .run()

  private def queueRequest(request: HttpRequest): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    queue.offer(request -> responsePromise).flatMap {
      case QueueOfferResult.Enqueued => responsePromise.future
      case QueueOfferResult.Dropped =>
        responsePromise.future //we dropped an element, but the new item can proceed
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.QueueClosed =>
        Future.failed(
          new RuntimeException(
            "Queue was closed (pool shut down) while running the request. Try again later."
          )
        )
    }
  }

  override def getDataSource(url: String): Future[Source[Double, Any]] =
    queueRequest(
      HttpRequest(uri = url)
        .withHeaders(RawHeader("Accept-Encoding", "gzip"))
    ).flatMap { response =>
      val stream = response.entity.dataBytes
        .via(Gzip.decoderFlow)
        .via(new ByteStringChunker(8))
        .map(bs => getDouble(bs.toArray))
        .via(OptionFilter)

      stream.runWith(Sink.seq).map(Source(_))
    }

  override def getEventSource(url: String): Future[Source[Long, Any]] =
    queueRequest(
      HttpRequest(uri = url)
        .withHeaders(RawHeader("Accept-Encoding", "gzip"))
    ).flatMap { response =>
      val stream = response.entity.dataBytes
        .via(Gzip.decoderFlow)
        .via(new ByteStringChunker(9))
        .map(bs => getLong(bs.toArray.dropRight(1))) // for now, ignore unit classification that follows the event timestamp
        .via(OptionFilter)

      stream.runWith(Sink.seq).map(Source(_))
    }
}

/**
  * A websocket client that assumes all file URLs are local filepaths,
  * and will stream the requested data straight from the local filesystem
  */
class LocalFilesystemWsClient(
  implicit
  val ec: ExecutionContext
) extends WsClient {
  override def getDataSource(url: String): Future[Source[Double, Any]] =
    Future.successful(
      FileIO
        .fromPath(Paths.get(url))
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
        .map(_.utf8String.toDouble)
    )

  override def getEventSource(url: String): Future[Source[Long, Any]] =
    Future.successful(
      FileIO
        .fromPath(Paths.get(url))
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
        .map(_.utf8String.toLong)
    )
}
