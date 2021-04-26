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
