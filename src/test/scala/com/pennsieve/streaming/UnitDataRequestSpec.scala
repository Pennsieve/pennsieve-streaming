package com.pennsieve.streaming

import akka.stream.scaladsl.{ Sink, Source }
import com.blackfynn.streaming.UnitRangeEntry
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.chunker.PredicateStreamChunker
import com.pennsieve.streaming.query.{
  LocalFilesystemWsClient,
  TimeSeriesUnitQueryRawHttp,
  UnitRangeRequest
}
import org.scalatest.FlatSpec

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by jsnavely on 5/5/17.
  */
class UnitDataRequestSpec extends FlatSpec with AkkaImplicits with TestConfig {

  implicit val log = new ContextLogger()

  val wsClient = new LocalFilesystemWsClient
  val unitQuery = new TimeSeriesUnitQueryRawHttp(config, wsClient)

  "requesting unit time data" should "get it" in {

    val url = "src/test/resources/events"

    val start = 0L
    val end = Long.MaxValue
    val channel = "N:channel:d1724e92-f236-42dd-a63e-099ec2630542"

    val pixelWidth = 100000

    val spikeDuration = 1733
    val spikeCount = 52
    val sampleRate = 30000

    val emptyLookup = UnitRangeEntry(-1, 0, 0, channel, 0, "", "")

    val rr = UnitRangeRequest(
      channel,
      1,
      100,
      None,
      pixelWidth,
      spikeDuration,
      spikeCount,
      sampleRate,
      emptyLookup,
      Some(1),
      Some(0)
    )

    val events = for {
      eventSource <- wsClient.getEventsSummary(url, pixelWidth, start, end)
      _events <- eventSource.runWith(Sink.seq)
    } yield _events
    val eventsIt = Await.result(events, 10.seconds)

    val waves = for {
      waveSource <- wsClient
        .getSpikes(url, 100L, spikeCount, spikeDuration)
      _waves <- waveSource.runWith(Sink.seq)
    } yield _waves
    val wavesIt = Await.result(waves, 10 seconds)

    val expectedSpikes = List(
      Vector(
        (0.0, 1.19974637e8),
        (1.06728613e8, 9.67618823e8),
        (1.57007241e8, 3.17041814e8),
        (1.31918108e8, 5.87785243e8),
        (1.31918108e8, 7.13118493e8),
        (1.80543303e8, 5.87785243e8),
        (1.57007241e8, 9.5105654e8),
        (1.06728613e8, 9.67618823e8),
        (1.06953895e8, 5.8139775e8),
        (1.06728613e8, 9.67618823e8),
        (1.57007241e8, 3.17041814e8),
        (1.31918108e8, 5.87785243e8),
        (1.31918108e8, 7.13118493e8),
        (1.80543303e8, 5.87785243e8),
        (1.57007241e8, 9.5105654e8),
        (1.06728613e8, 9.67618823e8),
        (1.1627955e8, 1.19974637e8)
      )
    )

    assert(wavesIt.take(1).toList == expectedSpikes)

    val nospikes = List[Seq[(Double, Double)]]()

    val times = eventsIt.toList.map(s => (s.avgTime, s.count))

    val _range = for {
      start <- eventsIt.headOption
      end <- eventsIt.lastOption
    } yield (start.minIndex, end.maxIndex)

    val range: (Long, Long) = _range getOrElse (0, 0)
    val ev = unitQuery.buildEvents(times, nospikes, range, rr)

    assert(ev.event.get.times == Seq(0, 1, 113464266, 2, 534781953, 947, 552674525, 300))

    assert(range._1 == 0)
    assert(range._2 == 1249)

    assert(ev.totalResponses == 1)
    assert(ev.responseSequenceId == 0)

  }

  "chunking times" should "work" in {

    val times = 1 to 100

    val longEnough = (s: Seq[Int], last: Int) => {
      val le = for {
        first <- s.headOption
      } yield (last - first) >= 5

      le.getOrElse(false)
    }

    val pchunker = new PredicateStreamChunker[Int](longEnough)
    val chunked =
      Await.result(Source(times).via(pchunker).runWith(Sink.seq), 10 seconds)

    assert(chunked.take(2) == Vector(Vector(1, 2, 3, 4, 5), Vector(6, 7, 8, 9, 10)))
  }

}
