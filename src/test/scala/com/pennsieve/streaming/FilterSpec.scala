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

import java.io.{ File, PrintWriter }

import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.FilterRequest
import org.scalatest.FlatSpec
import uk.me.berndporr.iirj.{ Butterworth, Cascade }

class FilterSpec extends FlatSpec {

  def writeToFile(nums: Vector[Double], fname: String) = {
    val writer = new PrintWriter(new File(fname))
    nums.foreach(n => writer.println(n))
    writer.close()
  }

  def readFromFile(fname: String): Vector[Double] = {
    val source = scala.io.Source.fromFile(fname)
    val output = source.getLines().map(_.toDouble).toVector
    source.close()
    output
  }

  def notchFilter(
    data: Seq[Double],
    sampleRate: Double,
    filterFreq: Double,
    width: Double = 3
  ): Seq[Double] = {
    val butterworth = new Butterworth()
    butterworth.bandStop(4, sampleRate, filterFreq, width)
    data.map(d => butterworth.filter(d))
  }

  "the butterworth bandstop filter" should "filter out the requested band" in {
    val path = getClass.getResource("/combined.txt").toURI.getPath
    val data = readFromFile(path)
    val filtered = notchFilter(data, 250.0, 50.0)

    assert(
      filtered.take(10) == Vector(0.0, 1.087161635562614, 0.9029426242294433, 0.20389260356351335,
        0.21682008432239974, 0.9644012648863328, 1.4729224362016369, 1.2208633250496284,
        0.6931728654057703, 0.5172583945876954)
    )

    //writeToFile(filtered.toVector,"/tmp/filtered.txt")

  }

  "a filter request" should "be parsed" in {
    import spray.json._

    val reqString =
      "{\"filter\":\"bandstop\",\"filterParameters\":[4,50,10],\"channels\":[\"N:channel:7a6ef1ca-da7b-443b-9bdb-3b5e2370f9e5\"]}"
    val req = reqString.parseJson.convertTo[FilterRequest]
    assert(req.filterParameters == List(4.0, 50.0, 10.0))

    println(req)

  }

  def printBQ(cs: Cascade): Unit = {
    for (i <- 0 to cs.getNumBiquads - 1) {
      val bq = cs.getBiquad(i)
      println(bq.getA0)
      println(bq.getA1)
      println(bq.getA2)
      println(bq.getB0)
      println(bq.getB1)
      println(bq.getB2)
      println("-------------------")
    }
  }
}
