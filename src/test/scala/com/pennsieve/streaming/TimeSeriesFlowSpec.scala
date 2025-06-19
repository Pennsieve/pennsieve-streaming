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

package com.pennsieve.streaming

import java.util.concurrent.ConcurrentHashMap
import akka.http.scaladsl.model.ws.TextMessage.Strict
import akka.http.scaladsl.model.ws.{ BinaryMessage, Message, TextMessage }
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.{ Sink, Source }
import com.pennsieve.models.Channel
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.{ LocalFilesystemWsClient, TimeSeriesQueryUtils }
import com.pennsieve.streaming.query.TimeSeriesQueryUtils._
import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.TimeSeriesFlow.{
  SessionFilters,
  SessionKillSwitches,
  SessionMontage
}
import com.pennsieve.streaming.server._
import org.scalatest.{ Inspectors, Matchers }
import org.scalatest.concurrent.ScalaFutures

import org.scalatest.fixture.FlatSpec
import spray.json._
import uk.me.berndporr.iirj.{ Butterworth }

import scala.collection.JavaConverters._
import scala.collection.{ concurrent, immutable }
import scala.concurrent.Await
import scala.concurrent.duration._

class TimeSeriesFlowSpec
    extends FlatSpec
    with Matchers
    with Inspectors
    with AkkaImplicits
    with TestConfig
    with TestDatabase
    with SessionGenerator
    with ScalaFutures {

  implicit val log = new ContextLogger()
  implicit val wsClient = new LocalFilesystemWsClient
  implicit val ports = new TestWebServerPorts

  val rangeLookUp =
    new RangeLookUp(ports.rangeLookupQuery, config.getString("timeseries.s3-base-url"))
  val unitRangeLookUp =
    new UnitRangeLookUp(ports.unitRangeLookupQuery, config.getString("timeseries.s3-base-url"))
  val sessionFilters: SessionFilters =
    new ConcurrentHashMap[String, concurrent.Map[String, FilterStateTracker]]().asScala
  val montage: SessionMontage =
    new ConcurrentHashMap[String, concurrent.Map[String, MontageType]]().asScala

  val sessionKillSwitches: SessionKillSwitches =
    new ConcurrentHashMap[String, concurrent.Map[Long, SharedKillSwitch]]().asScala

  "paginated channel data" should "stream through the system unaffected" in { implicit dbSession =>
    val channelId = ports.GenericIds.head
    val channelName = ports.GenericNames.head
    val session = getRandomSession()

    // requests for two adjacent pages
    val request = TextMessage(
      TimeSeriesRequest(
        packageId = ports.GenericPackage,
        virtualChannels = Some(List(VirtualChannel(id = channelId, name = channelName))),
        startTime = 200000000,
        endTime = 600000000,
        pixelWidth = 0
      ).toJson.toString
    )

    val requestSource = List(request)

    val tsFlow = new TimeSeriesFlow(
      session,
      sessionFilters,
      montage,
      sessionKillSwitches,
      channelMap = ports
        .packageMap(ports.GenericPackage)
        .map(chan => chan.nodeId -> chan)
        .toMap,
      rangeLookUp,
      unitRangeLookUp
    )

    val runfuture =
      Source[Message](requestSource)
        .via(tsFlow.flowGraph)
        .runWith(Sink.seq)

    val result = Await
      .result(runfuture, 10 seconds)
      .map(ports.parseProtobufFromMessage(TimeSeriesMessage.parseFrom))
    assert(result.length == 2)

    val page1 = result.head
    val page2 = result(1)

    assert(page1.segment.get.source == channelId)
    assert(page1.segment.get.channelName == channelName)
    assert(page1.segment.get.data.length == 200)
    assert(page1.segment.get.nrPoints == 200)

    assert(page2.segment.get.source == channelId)
    assert(page2.segment.get.channelName == channelName)
    assert(page2.segment.get.data.length == 200)
    assert(page2.segment.get.nrPoints == 200)
  }

  "aborting message" should "prevent previous messages from being handled" in {
    implicit dbSession =>
      val channelId = ports.GenericIds.head
      val channelName = ports.GenericNames.head
      val session = getRandomSession()

      // requests for two adjacent pages
      val request = TextMessage(
        TimeSeriesRequest(
          packageId = ports.GenericPackage,
          virtualChannels = Some(List(VirtualChannel(id = channelId, name = channelName))),
          startTime = 200000000,
          endTime = 600000000,
          pixelWidth = 0
        ).toJson.toString
      )

      // requests for two adjacent pages
      val abortRequest = TextMessage(DumpBufferRequest().toJson.toString)

//    val requestSource = (1 to 100).map(_ => request).toList
      val requestSource = List(request, request, abortRequest)

      val tsFlow = new TimeSeriesFlow(
        session,
        sessionFilters,
        montage,
        sessionKillSwitches,
        channelMap = ports
          .packageMap(ports.GenericPackage)
          .map(chan => chan.nodeId -> chan)
          .toMap,
        rangeLookUp,
        unitRangeLookUp
      )

      val runfuture =
        Source[Message](requestSource)
          .via(tsFlow.flowGraph)
          .concat(Source.empty.delay(200.milliseconds))
          .runWith(Sink.seq)

      whenReady(runfuture, timeout(5.seconds)) { messages =>
        // Parse the messages to understand what we received
        val textMessages = messages.collect { case tm: TextMessage => tm }
        val binaryMessages = messages.collect { case bm: BinaryMessage => bm }

        // When a dump buffer request is processed, we should get:
        // 1. A BufferDumpedResponse (as TextMessage)
        // 2. NO TimeSeriesMessage responses (as BinaryMessage) for the original request

        println(s"Total messages received: ${messages.length}")
        println(s"Text messages: ${textMessages.length}")
        println(s"Binary messages: ${binaryMessages.length}")

        textMessages.length shouldBe 0

        binaryMessages.length shouldBe 0

      }

  }

  "channel filters" should "be applied to the stream" in { implicit dbSession =>
    val channelId = ports.GenericIds.head
    val channelName = ports.GenericNames.head
    val session = getRandomSession()

    val channelFilters = new ConcurrentHashMap[String, FilterStateTracker]().asScala
    val MAX_FREQ = 1.0

    val butterworth = new Butterworth()
    butterworth.lowPass(4, 100, MAX_FREQ)

    channelFilters.put(channelId, new FilterStateTracker(butterworth, 4, MAX_FREQ))
    sessionFilters.put(session, channelFilters)

    // requests for two adjacent pages
    val request = TextMessage(
      TimeSeriesRequest(
        packageId = ports.GenericPackage,
        virtualChannels = Some(List(VirtualChannel(id = channelId, name = channelName))),
        startTime = 200000000,
        endTime = 600000000,
        pixelWidth = 0
      ).toJson.toString
    )

    val requestSource = List(request)

    val tsFlow = new TimeSeriesFlow(
      session,
      sessionFilters,
      montage,
      sessionKillSwitches,
      channelMap = ports
        .packageMap(ports.GenericPackage)
        .map(chan => chan.nodeId -> chan)
        .toMap,
      rangeLookUp,
      unitRangeLookUp
    )

    val runfuture =
      Source[Message](requestSource)
        .via(tsFlow.flowGraph)
        .runWith(Sink.seq)

    val result = Await
      .result(runfuture, 10 seconds)
      .map(ports.parseProtobufFromMessage(TimeSeriesMessage.parseFrom))
    assert(result.length == 2)

    val Seq(page1, page2) = result

    assert(page1.segment.get.source == channelId)
    assert(page1.segment.get.channelName == channelName)
    assert(page1.segment.get.data.length == 200)
    assert(page1.segment.get.nrPoints == 200)

    assert(page2.segment.get.source == channelId)
    assert(page2.segment.get.channelName == channelName)
    assert(page2.segment.get.data.length == 200)
    assert(page2.segment.get.nrPoints == 200)

//    all(page1.segment.get.data.map(Math.abs)) should be < MAX_FREQ
//    all(page2.segment.get.data.map(Math.abs)) should be < MAX_FREQ
  }

  "startAtEpoch" should "reset all timestamps to the beginning of the epoch for continuous channels" in {
    implicit dbSession =>
      val session = getRandomSession()

      // requests for two adjacent pages for both channels, starting
      // from 0.
      //
      // The first channel goes from 200000000 - 600000000, and the
      // second goes from 300000000 to 700000000, so this package
      // should be 'zeroed out' at 200000000 (the  start time
      // for the entire package)
      val request = TextMessage(
        TimeSeriesRequest(
          packageId = ports.GenericPackage,
          virtualChannels = Some(
            ports.GenericMap
              .filter(_._2 contains "continuous")
              .map {
                case (id, name) => VirtualChannel(id = id, name = name)
              }
              .toList
          ),
          startTime = 0,
          endTime = 400000000,
          pixelWidth = 0
        ).toJson.toString
      )

      val requestSource = List(request)

      val tsFlow = new TimeSeriesFlow(
        session,
        sessionFilters,
        montage,
        sessionKillSwitches,
        channelMap = ports
          .packageMap(ports.GenericPackage)
          .map(chan => chan.nodeId -> chan)
          .toMap,
        rangeLookUp,
        unitRangeLookUp,
        startAtEpoch = true
      )

      val runfuture =
        Source[Message](requestSource)
          .via(tsFlow.flowGraph)
          .runWith(Sink.seq)

      val result = Await
        .result(runfuture, 10 seconds)
        .map(ports.parseProtobufFromMessage(TimeSeriesMessage.parseFrom))
      assert(result.length == 4) // two responses for each channel

      val fieldsToTest = result.map(
        msg =>
          (
            msg.segment.get.source,
            msg.segment.get.nrPoints,
            msg.segment.get.startTs,
            msg.segment.get.pageStart,
            msg.segment.get.pageEnd,
            msg.responseSequenceId
          )
      )
      val expected = List(
        // first three channels should contain all of their data - but starting from 0
        ("paginated_continuous_ch1_id", 200, 0, 0, 400000000, 0),
        ("paginated_continuous_ch1_id", 200, 200000000, 0, 400000000, 1),
        ("paginated_continuous_ch2_id", 200, 100000000, 0, 400000000, 0),
        // last channel should be truncated
        ("paginated_continuous_ch2_id", 100, 300000000, 0, 400000000, 1)
      )

      fieldsToTest should contain theSameElementsAs (expected)
  }

  "startAtEpoch" should "reset all timestamps to the beginning of the epoch for unit channels" in {
    implicit dbSession =>
      val session = getRandomSession()

      val request = TextMessage(
        TimeSeriesRequest(
          packageId = ports.GenericPackage,
          virtualChannels = Some(
            ports.GenericMap
              .filter(_._2 contains "unit")
              .map {
                case (id, name) => VirtualChannel(id = id, name = name)
              }
              .toList
          ),
          startTime = 0,
          endTime = 400000000,
          pixelWidth = 1
        ).toJson.toString
      )

      val requestSource = List(request)

      val tsFlow = new TimeSeriesFlow(
        session,
        sessionFilters,
        montage,
        sessionKillSwitches,
        channelMap = ports
          .packageMap(ports.GenericPackage)
          .map(chan => chan.nodeId -> chan)
          .toMap,
        rangeLookUp,
        unitRangeLookUp,
        startAtEpoch = true
      )

      val runfuture =
        Source[Message](requestSource)
          .via(tsFlow.flowGraph)
          .runWith(Sink.seq)

      val result = Await
        .result(runfuture, 10 seconds)
        .map(ports.parseProtobufFromMessage(TimeSeriesMessage.parseFrom))
      assert(result.length == 4)
      val fieldsToTest = result.map { msg =>
        (
          msg.event.get.source,
          msg.event.get.pageStart,
          msg.event.get.pageEnd,
          msg.event.get.times.length,
          msg.event.get.data.length,
          msg.responseSequenceId
        )
      }

      val expected = List(
        // first three channels should contain all data starting from 0
        ("paginated_unit_ch1_id", 0, 400000000, 400, 400, 0),
        ("paginated_unit_ch1_id", 0, 400000000, 400, 400, 1),
        ("paginated_unit_ch2_id", 0, 400000000, 400, 400, 0),
        // the last channel only contains 99 rows that are under our maximum
        ("paginated_unit_ch2_id", 0, 400000000, 198, 198, 1)
      )

      fieldsToTest should contain theSameElementsAs (expected)

      val timesToTest = result.map(msg => msg.event.get.times)

      timesToTest foreach { times =>
        times
          .grouped(2)
          .foreach {
            case Seq(timestamp, count) =>
              // the timestamps themselves should only exist within
              // these boundaries
              timestamp should be >= 0L
              timestamp should be < 400000000L

              count should be(1)
          }

      }
  }

  "montaged channels" should "be combined into a single data stream" in { implicit dbSession =>
    val leadChannelId = "Fp1_id"

    val montageName = "Fp1<->Cz"
    val montageSession = "montage_session"

    val request = TextMessage(
      TimeSeriesRequest(
        packageId = ports.MontagePackage,
        virtualChannels = Some(List(VirtualChannel(id = leadChannelId, name = montageName))),
        startTime = 0,
        endTime = 100,
        pixelWidth = 0
      ).toJson.toString
    )

    val montageRequest = TextMessage(
      MontageRequest(packageId = ports.MontagePackage, montage = MontageType.ReferentialVsCz).toJson.toString
    )

    val requestSource = List(montageRequest, request)

    val tsFlow = new TimeSeriesFlow(
      montageSession,
      sessionFilters,
      montage,
      sessionKillSwitches,
      channelMap = ports.MontageIds.map(id => id -> ports.createDummyChannel(id)).toMap,
      rangeLookUp,
      unitRangeLookUp
    )

    val runfuture =
      Source[Message](requestSource)
        .via(tsFlow.flowGraph)
        .runWith(Sink.seq)

    val result: immutable.Seq[Message] = Await.result(runfuture, 10 seconds)
    assert(result.length == 2)

    val montageList = ports.parseJsonFromMessage[ChannelsDetailsList](result.head)
    val timeSeriesMessage =
      ports
        .parseProtobufFromMessage(TimeSeriesMessage.parseFrom)(result.last)
        .segment
        .get

    montageList.channelDetails.map(_.name) should contain theSameElementsAs (MontageType.ReferentialVsCz.names)

    assert(timeSeriesMessage.source == leadChannelId)
    assert(timeSeriesMessage.channelName == montageName)
    assert(
      timeSeriesMessage.data == Vector(0.0, -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0,
        -10.0)
    )
  }

  "sending a request for non-existent data" should "return an empty response in the query flow" in {
    implicit dbSession =>
      val channelName = ports.NonexistentChannelNames.head
      val channelId = ports.NonexistentChannelIds.head

      val tsFlow = new TimeSeriesFlow(
        getRandomSession(),
        sessionFilters,
        montage,
        sessionKillSwitches,
        ports.NonexistentChannelIds
          .map(id => id -> ports.createDummyChannel(id))
          .toMap,
        rangeLookUp,
        unitRangeLookUp
      )

      val tsquery =
        s"""{
             "session":"c5ca51eb-83e7-423a-9932-d21a4859b2f4",
             "virtualChannels":[{"id": "$channelId", "name": "$channelName"}],
             "minMax":true,
             "startTime":1485889717000000,
             "endTime":1485889722000000,
             "packageId":"${ports.NonexistentChannelPackage}",
             "pixelWidth":10170
           }"""

      val inMessage: Message = Strict(tsquery)

      val s = Segment(
        startTs = 1485889717000000L,
        source = channelId,
        pageStart = 1485889717000000L,
        pageEnd = 1485889722000000L,
        segmentType = "Continuous",
        requestedSamplePeriod = 10170,
        channelName = channelName
      )
      val expected =
        TimeSeriesMessage(segment = Some(s), totalResponses = 1, responseSequenceId = 0)

      val result = Source[Message](List(inMessage))
        .via(tsFlow.flowGraph)
        .take(1)
        .runWith(Sink.seq)

      val observed = Await.result(result, 10 seconds).head match {
        case BinaryMessage.Strict(bytes) =>
          Right(TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array))
        case other => Left(other)
      }

      assert(observed == Right(expected))
  }

  "sending a request for non-existent neural data" should "return an empty response in the query flow" in {
    implicit dbSession =>
      val channelId = "N:c:797d784e8736466aa28ad8762f70d1a2"
      val channel = Channel(channelId, 0, "realtimechan", 0, 0, "V", 200.0, "unit", None, 123L)
      val cmap = Map(channelId -> channel)

      val tsFlow =
        new TimeSeriesFlow(
          getRandomSession(),
          sessionFilters,
          montage,
          sessionKillSwitches,
          cmap,
          rangeLookUp,
          unitRangeLookUp
        )

      val tsquery =
        """{
             "session":"c5ca51eb-83e7-423a-9932-d21a4859b2f4",
             "virtualChannels":[{"id": "N:c:797d784e8736466aa28ad8762f70d1a2", "name": "realtimechan"}],
             "minMax":true,
             "startTime":1485889717000000,
             "endTime":1485889722000000,
             "packageId":"N:fo:662b0000-e085-4f36-8063-fda3dda0af89",
             "pixelWidth":10170,
             "queryLimit":0
           }"""

      val inMessage: Message = Strict(tsquery)

      val e = Event(
        pageStart = 1485889717000000L,
        pageEnd = 1485889722000000L,
        source = "N:c:797d784e8736466aa28ad8762f70d1a2",
        samplePeriod = 10170
      )

      val expected = TimeSeriesMessage(event = Some(e))

      val result = Source[Message](List(inMessage))
        .via(tsFlow.flowGraph)
        .take(1)
        .runWith(Sink.seq)

      val observed = Await.result(result, 10 seconds).head match {
        case BinaryMessage.Strict(bytes) =>
          Right(TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array))
        case other => Left(other)
      }

      assert(observed == Right(expected))
  }

  "resampling" should "occur when the requested sample period is larger than the ingest sample period" in {
    _ =>
      val data = 1 to 100 map (_.toDouble)
      val i = IngestSegment("abc", 0L, 10000, data)

      val tmresample = resampleMessage(i, 10001L)

      val firstTen = tmresample.data.take(10)
      assert(tmresample.isMinMax)
      assert(firstTen == Vector(1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0, 5.0))
  }

  "resampling" should "return an accurate sample period" in { _ =>
    val data = 1 to 100 map (_.toDouble)
    val pixelDuration = 30001L
    val startTime = 0L
    val priorSamplePeriod = 10000
    val endTime = startTime + (priorSamplePeriod * data.size)

    val chunkCount = TimeSeriesQueryUtils
      .calculateChunks(startTime, endTime, pixelDuration)
    assert(chunkCount == 33)

    val expectedSamplePeriod = TimeSeriesQueryUtils
      .calculateSamplePeriod(startTime, endTime, chunkCount)
    assert(Math.round(expectedSamplePeriod * 10000) / 10000.0 == 30303.0303)

    val i = IngestSegment("abc", startTime, priorSamplePeriod, data)

    val resampled = resampleMessage(i, pixelDuration)

    assert(resampled.isMinMax)
    assert(resampled.samplePeriod == expectedSamplePeriod)
  }

  "resampling" should "NOT occur when the requested sample period is equal the ingest sample period" in {
    _ =>
      val data = 1 to 100 map (_.toDouble)
      val i = IngestSegment("abc", 0L, 10000, data)

      val tmresample = resampleMessage(i, 10000L)

      val firstTen = tmresample.data.take(10)
      assert(!tmresample.isMinMax)
      assert(firstTen == Vector(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))
  }

  "resampling" should "NOT occur when the requested sample period is less than the ingest sample period" in {
    _ =>
      val data = 1 to 100 map (_.toDouble)
      val i = IngestSegment("abc", 0L, 10000, data)

      val tmresample = resampleMessage(i, 1000L)

      val firstTen = tmresample.data.take(10)
      assert(!tmresample.isMinMax)
      assert(firstTen == Vector(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))
  }
}
