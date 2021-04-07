/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.pennsieve.streaming.query

import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream, InputStream }
import java.nio.file.{ Files, Paths }

import akka.stream.scaladsl.Source
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.domain.Error
import com.pennsieve.service.utilities.ContextLogger
import com.blackfynn.streaming.util.bytesToDouble
import com.blackfynn.streaming.{ IngestSegment, LookupResultRow, Segment }
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

/**
  * Created by jsnavely on 1/13/17.
  */
object TimeSeriesQueryUtils {

  def readBinaryFile(filename: String): Vector[Double] = {
    val f = new FileInputStream(filename)
    val bis: BufferedInputStream = new BufferedInputStream(f)
    readBinaryStream(bis)
  }

  def readBytes(fname: String): Array[Byte] = {
    val path = Paths.get(fname)
    Files.readAllBytes(path)
  }

  def readBinaryStream(is: InputStream): Vector[Double] = {

    val bytes: Iterator[Byte] = Iterator
      .continually(is.read())
      .takeWhile(-1 !=)
      .map(_.toByte)

    bytesToDouble(bytes)
  }

  def writeBinaryFile(data: List[Double], filename: String) = {
    val f = new FileOutputStream(filename)
    val bytes = new Array[Byte](8)
    val bb = java.nio.ByteBuffer.wrap(bytes)
    bb.rewind()
    data foreach { d =>
      {
        bb.clear()
        bb.putDouble(d)
        f.write(bytes)
      }
    }
    f.flush()
    f.close()
  }

  def fillGap(target: (Double, Double), context: (Double, Double)): (Double, Double) = {

    val (min1, max1) = target
    val (min2, max2) = context

    if (contains(target, context) || contains(context, target)) {
      (min1, max1)
    } else if (max1 < min2) {
      (min1, min2)
    } else if (min1 > max2) {
      (max2, max1)
    } else {
      (min1, max1)
    }
  }

  def fillGaps(minmax: Vector[(Double, Double)]): Vector[(Double, Double)] = {

    if (minmax.isEmpty) {
      Vector()
    } else {
      val last = flip(minmax.last)
      val padded = minmax :+ last

      padded
        .sliding(2)
        .map(pair => {
          fillGap(pair(0), pair(1))
        })
        .toVector
    }
  }

  //trims data vector based on how the query overlaps with the results
  // assumes starts and ends in microsecond utc
  //rate in hz
  //returns: start, end, data
  def trim[A](
    resultStart: Long,
    resultEnd: Long,
    queryStart: Long,
    queryEnd: Long,
    sampleRate: Double,
    v: Source[A, Any]
  )(implicit
    log: ContextLogger
  ): (Long, Long, Source[A, Any]) = {

    val period = 1e6 / sampleRate ///the number of microseconds between sample

    val rd = ResultDimensions(resultStart, resultEnd, queryStart, queryEnd)

    if (rd.queryDisjoint) {
      (0, 0, Source(Vector())) //this would be pretty weird
    } else if (rd.queryRich) {
      val startDiff =
        Math.round(Math.abs(queryStart - resultStart) / period).toInt
      val expectedLength = Math.round((queryEnd - queryStart) / period).toInt
      val data = v.drop(startDiff).take(expectedLength)
      (queryStart, queryEnd, data)
    } else if (rd.queryOverlapRight) {
      val expectedLength = Math.round((queryEnd - resultStart) / period).toInt
      val data = v.take(expectedLength)
      (resultStart, queryEnd, data)
    } else if (rd.queryOverlapLeft) {
      val startDiff =
        Math.round(Math.abs(queryStart - resultStart) / period).toInt
      val data = v.drop(startDiff)
      (queryStart, resultEnd, data)
    } else if (rd.queryPoor) {
      (resultStart, resultEnd, v)
    } else if (rd.queryPerfect) {
      (queryStart, queryEnd, v)
    } else {
      log.noContext.error("Got an unexpected query range condition")
      (0, 0, Source(Vector()))
    }
  }

  def sampleCount(duration: Long, sampleRate: Double): Long = {
    math.round((duration / 1e6) * sampleRate)
  }

  def sampleCount(lu: LookupResultRow): Long =
    sampleCount(lu.max - lu.min, lu.sampleRate)

  def toMicrosecondUTC(dt: DateTime): Long = {
    dt.getMillis * 1000
  }

  def flip[A](pair: (A, A)): (A, A) = (pair._2, pair._1)

  def contains(p1: (Double, Double), p2: (Double, Double)): Boolean = {
    val (min1, max1) = p1
    val (min2, max2) = p2
    (min1 <= min2) && (max1 >= max2)
  }

  def shouldResample(rate: Double, microsecondsPerPixel: Long): Boolean = {
    val microsecondPerDatapoints = (1e6 / rate)
    val ratio: Double = microsecondsPerPixel.toDouble / microsecondPerDatapoints

    // we must have at least 3 datapoints per sample period before we
    // start to resample. otherwise, send raw data.
    ratio > 3.0
  }

  def findMinMax(v: Vector[Double]): Option[(Double, Double)] = {
    for {
      first <- v.headOption
    } yield
      v.drop(1).foldLeft((first, first)) {
        case (mm, v) =>
          (Math.min(mm._1, v), Math.max(mm._2, v))
      }
  }

  def findMinMaxUnsafe(v: Seq[Double]): (Double, Double) = {
    val first = v.head
    v.drop(1).foldLeft((first, first)) {
      case (mm, v) =>
        (math.min(mm._1, v), math.max(mm._2, v))
    }
  }

  def repeat(n: Long, count: Long): List[Long] = {
    1 to count.toInt map { _ =>
      n
    } toList
  }

  def cut[A](xs: Vector[A], n: Int): Vector[Vector[A]] = {

    val m = xs.length
    val targets = (0 to n).map { x =>
      math.round((x.toDouble * m) / n).toInt
    }.toVector

    @tailrec
    def snip(xs: Vector[A], ns: Vector[Int], got: Vector[Vector[A]]): Vector[Vector[A]] = {
      if (ns.length < 2) {
        got
      } else {
        val (i, j) = (ns.head, ns.tail.head)
        snip(xs.drop(j - i), ns.tail, got :+ xs.take(j - i))
      }
    }

    snip(xs, targets, Vector.empty)
  }

  def resample(data: Vector[Double], sampleCount: Int): Seq[(Double, Double)] = {
    if (data.isEmpty) {
      Seq()
    } else {
      cut(data, sampleCount)
        .collect({ case x if x.length > 0 => findMinMaxUnsafe(x) })
    }
  }

  def calculateChunks(start: Long, end: Long, pixelDuration: Long): Int =
    Math.round((end - start) / pixelDuration)

  def calculateSamplePeriod(start: Long, end: Long, chunkCount: Long): Double =
    (end - start) / chunkCount.toDouble

  def resampleMessage(ingest: IngestSegment, realtime_pixel_duration: Long): Segment = {

    val shouldResample = ingest.samplePeriod < realtime_pixel_duration

    if (shouldResample) {
      val data = ingest.data.toVector

      val endTime = ingest.startTime + (ingest.samplePeriod * data.size) toLong

      val chunkCount =
        calculateChunks(ingest.startTime, endTime, realtime_pixel_duration)

      val resampled = resample(ingest.data.toVector, chunkCount) flatMap { pair =>
        List(pair._1, pair._2)
      }

      val samplePeriod =
        calculateSamplePeriod(ingest.startTime, endTime, chunkCount)

      Segment(
        source = ingest.channelId,
        startTs = ingest.startTime,
        samplePeriod = samplePeriod,
        requestedSamplePeriod = realtime_pixel_duration,
        isMinMax = true,
        segmentType = "realtime",
        nrPoints = chunkCount,
        data = resampled
      )
    } else {
      Segment(
        source = ingest.channelId,
        startTs = ingest.startTime,
        samplePeriod = ingest.samplePeriod,
        requestedSamplePeriod = realtime_pixel_duration,
        isMinMax = false,
        segmentType = "realtime",
        nrPoints = ingest.data.length,
        data = ingest.data
      )
    }

  }

  def first[A, B](ps: Iterable[(A, B)]): Iterable[A] = {
    ps map (p => p._1)
  }

  def second[A, B](ps: Iterable[(A, B)]): Iterable[B] = {
    ps map (p => p._2)
  }

  def trimToRange[T](rng: (Long, Long), vs: Iterable[T]): Iterable[T] = {
    val (start, end) = rng
    val numbered = vs.zipWithIndex
      .dropWhile(t => t._2 < start)
      .takeWhile(t => t._2 <= end)
    first(numbered)
  }

  def combine(
    threshold: Long,
    l1: Vector[(Long, Long)],
    l2: Vector[(Long, Long)]
  ): Vector[(Long, Long)] = {

    (l1, l2) match {
      case (v1, v2) if v1.isEmpty && v2.isEmpty => Vector.empty
      case (v1, v2) if v1.isEmpty => v2
      case (v1, v2) if v2.isEmpty => v1
      case (acc, toadd) => {
        val first = toadd(0)
        val last = acc.last
        if ((first._1 - last._2) >= threshold) {
          acc :+ first
        } else {
          acc.dropRight(1) :+ (last._1, first._2)
        }
      }
    }
  }

  def contig(pairs: Iterable[(Long, Long)], threshold: Long): List[(Long, Long)] =
    pairs
      .map(Vector(_))
      .fold(Vector()) { (l1, l2) =>
        combine(threshold, l1, l2)
      }
      .toList

  def timeGapThreshold(rate: Double, gapMultiple: Double): Long = {
    Math.floor((1e6 / rate) * gapMultiple).toLong
  }

  def findContiguousTimeSpans(
    rows: List[LookupResultRow],
    gapMultiple: Double = 2.0
  ): List[(Long, Long)] = {
    val timespans = for {
      first <- rows.headOption
    } yield {
      val thresh = timeGapThreshold(first.sampleRate, gapMultiple)
      val spans = rows.map(r => (r.min, r.max))
      contig(spans, thresh)
    }
    timespans.getOrElse(Nil)
  }

  def parseLong(
    l: String
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, Error, Long] = {
    Either
      .fromTry(Try(l.toLong))
      .toEitherT[Future]
      .leftMap(t => Error(t.getMessage))
  }

  def overLimit(start: Long, end: Long, rate: Double, limit: Long): Either[Error, String] = {
    val samples: Double = ((end - start) / 1e6) * rate
    if (samples <= limit) {
      Right("ok")
    } else {
      Left(Error(s"exceeded retrieval limit of $limit"))
    }
  }
}
