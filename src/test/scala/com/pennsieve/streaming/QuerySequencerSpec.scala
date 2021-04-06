package com.pennsieve.streaming

import akka.stream.scaladsl.Sink
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.domain.CoreError
import com.pennsieve.models.Channel
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.{
  LocalFilesystemWsClient,
  QuerySequencer,
  RangeRequest,
  UnitRangeRequest
}
import scalikejdbc.DBSession
import com.pennsieve.streaming.server.TestWebServerPorts
import org.scalatest.Matchers
import org.scalatest.fixture.WordSpec

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.io.Source

class QuerySequencerSpec
    extends WordSpec
    with Matchers
    with AkkaImplicits
    with TestConfig
    with TestDatabase
    with SessionGenerator {
  implicit val log = new ContextLogger()
  implicit val wsClient = new LocalFilesystemWsClient
  implicit val ports = new TestWebServerPorts

  val rangeLookUp =
    new RangeLookUp(ports.rangeLookupQuery, config.getString("timeseries.s3-base-url"))
  val unitRangeLookUp =
    new UnitRangeLookUp(ports.unitRangeLookupQuery, config.getString("timeseries.s3-base-url"))

  def continuousEmptyLookup(channel: Channel) =
    LookupResultRow(-1, channel.start, channel.end, channel.rate, channel.nodeId, "")

  def unitRangeEntry =
    UnitRangeEntry(0, min = 0, max = 0, channel = "", count = 0, tsindex = "", tsblob = "")

  def querySequencer(implicit dbSession: DBSession) =
    new QuerySequencer(rangeLookUp, unitRangeLookUp)

  "continuous range requests" should {
    "return a single datastream for a paginated channel" in { implicit dbSession =>
      val channelNodeId = "paginated_continuous_ch1_id"
      val channel = ports.GenericChannelMap(channelNodeId)

      val page1Source = Source.fromFile("src/test/resources/paginated/page1")
      val page2Source = Source.fromFile("src/test/resources/paginated/page2")

      val expectedEvents = (page1Source.getLines
        ++ page2Source.getLines).map(_.toDouble).toList

      val rangeRequest = RangeRequest(
        channelNodeId,
        channel.name,
        channel.start,
        channel.end,
        pixelWidth = 1,
        lookUp = continuousEmptyLookup(channel)
      )

      val events = for {
        source <- querySequencer.continuousRangeRequestT(rangeRequest)
        output <- EitherT.right[CoreError](source.runWith(Sink.seq))
      } yield output.map(_._2)

      val result = Await.result(events.value, 5 seconds)

      result.isRight should be(true)
      result.right.get should contain theSameElementsAs (expectedEvents)

      page1Source.close()
      page2Source.close()
    }
  }

  "unit range requests" should {
    "return a single datastream for a paginated channel" in { implicit dbSession =>
      val channelNodeId = "paginated_unit_ch1_id"
      val channel = ports.GenericChannelMap(channelNodeId)

      val page1Source =
        Source.fromFile("src/test/resources/paginated_events/ch1_page1")
      val page2Source =
        Source.fromFile("src/test/resources/paginated_events/ch1_page2")

      val expectedEvents = (page1Source.getLines
        ++ page2Source.getLines).map(_.toLong).toList

      val rangeRequest = UnitRangeRequest(
        channelNodeId,
        channel.start,
        channel.end,
        pixelWidth = 1,
        spikeDuration = 1,
        spikeDataPointCount = 1,
        sampleRate = 1.0,
        lookUp = unitRangeEntry
      )

      val events = for {
        source <- querySequencer.unitRangeRequestT(rangeRequest)
        output <- EitherT.right[CoreError](source.runWith(Sink.seq))
      } yield output

      val result = Await.result(events.value, 5 seconds)

      result.isRight should be(true)
      result.right.get should contain theSameElementsAs (expectedEvents)

      page1Source.close()
      page2Source.close()
    }
  }
}
