/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.pennsieve.streaming

import akka.stream.scaladsl.{ Sink, Source }
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.TimeSeriesQueryUtils._
import com.pennsieve.streaming.query.{
  LocalFilesystemWsClient,
  RangeRequest,
  TimeSeriesQueryRawHttp
}
import com.pennsieve.streaming.server.TestWebServerPorts
import org.joda.time.DateTime
import org.scalatest.fixture.FlatSpec

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by jsnavely on 9/28/16.
  */
class TimeSeriesRequestSpec extends FlatSpec with AkkaImplicits with TestDatabase with TestConfig {

  implicit val log = new ContextLogger()

  val wsClient = new LocalFilesystemWsClient
  val ports = new TestWebServerPorts
  val tsquery = new TimeSeriesQueryRawHttp(wsClient)
  val channelNodeId = "N:channel:0c961088-46b8-4468-b45e-472eaf19a893"
  val channelName = "channel"
  val data_start = 1301921822000000L
  val data_end = 1301922421995000L

  val rangeLookUp =
    new RangeLookUp(ports.rangeLookupQuery, config.getString("timeseries.s3-base-url"))

  def rangeQueryNow(rr: RangeRequest): TimeSeriesMessage = {
    Await.result(tsquery.rangeQuery(rr, mutable.Map()), 15.seconds)
  }

  //two seconds of data

  "filling gaps in a sequence" should "leave no gaps" in { _ =>
    val goodSeq: Vector[(Double, Double)] =
      Vector((0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 4.0))

    val filled = fillGaps(goodSeq)

    assert(goodSeq == filled)

    val badSeq = Vector((0.0, 1.0), (1.1, 2.0), (2.1, 3.0), (3.1, 4.0))

    val corrected = fillGaps(badSeq)

    assert(corrected == Vector((0.0, 1.1), (1.1, 2.1), (2.1, 3.1), (3.1, 4.0)))

    val badSeq2 = Vector((3.0, 4.0), (2.0, 2.9))

    val corrected2 = fillGaps(badSeq2)

    assert(corrected2 == Vector((2.9, 4.0), (2.0, 2.9)))

    //good vectors are left untouched
    val good2 = Vector((1.0, 10.0), (2.0, 9.0))
    assert(good2 == fillGaps(good2))

    val good3 = Vector((2.0, 9.0), (1.0, 10.0))
    assert(good3 == fillGaps(good3))
  }

  "trimming from the middle" should "get half the data" in { _ =>
    val data = 1 to 100 toVector

    val vals = List(1, 100, 51, 1000).map(_ * 1000000) //multiple by 1million because of microseconds

    val rstart = vals.head
    val rend = vals(1)
    val qstart = vals(2)
    val qend = vals(3)

    val (s, e, results) = trim(rstart, rend, qstart, qend, 1, Source(data))
    assert(s == qstart)
    assert(e == rend)

    val rs = Await.result(results.runWith(Sink.seq), 10 seconds)
    assert(rs.length == 50)
  }

  "trimming a result with data missing on both sides" should "get ALL the data" in { _ =>
    val data = 1 to 10 toVector

    val vals = List(10, 20, 1, 30).map(_ * 1000000) //multiple by 1million because of microseconds

    val rstart = vals.head
    val rend = vals(1)
    val qstart = vals(2)
    val qend = vals(3)

    val (s, e, results) = trim(rstart, rend, qstart, qend, 1, Source(data))
    assert(s == rstart)
    assert(e == rend)
    val rs = Await.result(results.runWith(Sink.seq), 10 seconds)

    assert(rs.length == 10)
  }

  "trimming a response" should "result in the right number of elements" in { _ =>
    val sampleRate = 250.0 //hz ( 1,000,000 / 250 = 4000 microsecond in between data points)

    val data = Range(0, 125000).toVector
      .map(_.toDouble)

    // 250 * 500 = 125,000 data points = 500 seconds or 8.3 minutes

    val bday = new DateTime(2017, 1, 23, 0, 0, 0)

    val rstart = toMicrosecondUTC(bday)
    val rend = toMicrosecondUTC(bday.plusSeconds(500))

    //trim 30 seconds on front and 30 seconds on end
    val qstart = toMicrosecondUTC(bday.plusSeconds(30))
    val qend = toMicrosecondUTC(bday.plusSeconds(470))

    val (_, _, trimmed) =
      trim(rstart, rend, qstart, qend, sampleRate, Source(data))

    //60 * 250 == 15000
    //125,000 - 15000 == 110,000

    val _trimmed = Await.result(trimmed.runWith(Sink.seq), 10 seconds)

    val trimmed_size = _trimmed.size
    assert(trimmed_size == 110000)

    //30 seconds * 250hz == 7500
    assert(_trimmed(0) - 7500.00 <= 0.00000)

    //the last data point is 124999
    //124999 - 7500 = 117499
    assert(_trimmed.last - 117499.0 <= 0.00)
  }

  "range lookup" should "return the range in postgres" in { implicit dbSession =>
    val qstart = data_start + (10 * 1000000)
    val qend = data_start + (12 * 1000000)

    val lkup = rangeLookUp.lookup(qstart, qend, channelNodeId)
    assert(lkup.head.sampleRate == 200)
    assert(lkup.head.min == 1301921822000000L)
    assert(lkup.head.max == 1301922421995000L)
  }

  "querying with a limit of zero" should "return no data" in { implicit dbSession =>
    val qstart = data_start + (10 * 1000000)
    val qend = data_start + (12 * 1000000)

    val lkup = rangeLookUp.lookup(qstart, qend, channelNodeId).head

    val result: TimeSeriesMessage =
      rangeQueryNow(RangeRequest(channelNodeId, channelName, qstart, qend, Some(0), 5000, lkup))
    val segmentData = result.segment.get.data

    assert(segmentData.size == 0)
    assert(result.segment.get.startTs == qstart)
    assert(result.segment.get.pageStart == qstart)
  }

  "finding the min/max of a group" should "work the same as finding the min and max individually" in {
    _ =>
      val nums = 1 to 100 map (_.toDouble) toVector
      val mm = findMinMaxUnsafe(nums)
      val simpleMinMax = (nums.min, nums.max)
      assert(mm == simpleMinMax)
  }

  "resample" should "resample " in { _ =>
    val nums: Vector[Double] = 1 to 100 map (_.toDouble) toVector

    // TODO: actually test the output of this
    resample(nums, 23)
  }

}
