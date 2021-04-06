/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.pennsieve.streaming

import java.io.ByteArrayOutputStream
import org.joda.time.DateTime
import org.scalatest.FlatSpec

/**
  * Created by jsnavely on 9/28/16.
  */
class SegmentProtobufSpec extends FlatSpec {

  val now = DateTime.now().getMillis
  val data = Range(0, 123) map { i =>
    i.toDouble
  }

  val segment = Segment(
    startTs = now,
    source = "abc123",
    lastUsed = now,
    unit = "V",
    samplePeriod = 1.23,
    pageStart = 123,
    isMinMax = false,
    unitM = 123,
    segmentType = "Continuous",
    nrPoints = 123,
    data = data
  )

  val tsMsg = TimeSeriesMessage(segment = Some(segment))

  "a segment" should "be properly decoded" in {

    val msgBytes = tsMsg.toByteArray

    val out = new ByteArrayOutputStream(msgBytes.length)
    tsMsg.writeTo(out)

    val newMsg = TimeSeriesMessage.parseFrom(out.toByteArray)

    assert(newMsg == tsMsg)
    assert(newMsg.segment.get.lastUsed == now)
    assert(newMsg.segment.get.segmentType == "Continuous")

  }

}
