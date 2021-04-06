/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.pennsieve.streaming

import org.scalatest.FlatSpec
import com.blackfynn.streaming.query.TimeSeriesQueryUtils._

/**
  * Created by jsnavely on 2/9/17.
  */
class IngestSegmentSpec extends FlatSpec {

  "parsing an ingest segment" should "result in the same segment" in {

    val i = IngestSegment(
      channelId = "abc123",
      startTime = 123L,
      samplePeriod = 1000.0,
      data = Range(1, 1000).map(_.toDouble)
    )

    val bytes = i.toByteArray

    val newSeg = IngestSegment.parseFrom(bytes)

    assert(i == newSeg)
  }

  "resampling realtime" should "produce no gaps" in {

    val random = scala.util.Random

    val sampleRate = 100 //hz

    val samplePeriod = 1000000 / sampleRate

    var totalDataPoints = 0L

    //create 1000 randomly sized ingest packets
    val packets: Seq[IngestSegment] = Range(0, 1000) map { _ =>
      val size = 20 + random.nextInt(20)
      val data = List.fill(size)(List(-10.0, 10.0)).flatten

      val ingest =
        IngestSegment("channel", totalDataPoints * samplePeriod.toLong, samplePeriod, data)

      totalDataPoints = totalDataPoints + data.size

      ingest
    }

    val dataTimes = packets.flatMap(p => {
      Range(0, p.data.size)
        .map(i => p.startTime + (i * p.samplePeriod.toLong))
        .toList
    })

    val gaps = dataTimes
      .sliding(2)
      .map(pp => pp(1) - pp(0))
      .toList
      .filter(p => p != 10000)

    //every datapoint is equally spaced before sampling
    assert(gaps.size == 0)

    val resampled: Seq[Segment] = packets.map(pkt => resampleMessage(pkt, 500))

    val resampledDataTimes = resampled.flatMap(
      rs =>
        Range(0, rs.data.size)
          .map(i => rs.startTs + (i * rs.samplePeriod.toLong))
    )

    val resampledGaps = resampledDataTimes
      .sliding(2)
      .map(pp => pp(1) - pp(0))
      .toList
      .filter(p => p != 10000)

    //every datapoint is equally spaced before after resampling

    assert(resampledGaps.size == 0)

  }

}
