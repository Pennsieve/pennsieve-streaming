package com.pennsieve.streaming

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.blackfynn.streaming.query.BaseTimeSeriesQuery
import com.blackfynn.streaming.query.TimeSeriesQueryUtils.contig
import org.scalatest.FlatSpec

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

class ResamplingSpec extends FlatSpec {

  implicit val system = ActorSystem("test-system")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  "filtering contiguous segments" should "return only segments with gaps" in {
    val ll = List((1L, 4L), (5L, 8L), (10L, 12L), (13L, 18L))
    val nobumps = contig(ll, 2)
    assert(nobumps == List((1L, 8L), (10L, 18L)))
  }

  @tailrec
  private def sliceEqual[A](s: Seq[A], acc: Seq[Seq[A]] = Seq()): Seq[Seq[A]] = {
    s match {
      case fst :: _ =>
        val (l, r) = s.span(fst ==)
        sliceEqual(r, acc :+ l)
      case Nil => acc
    }
  }

  "stream resampling" should "not move data" in {

    val sampleRate = 200.0d
    val pulseWidth = (sampleRate * 5).toInt

    assert(pulseWidth == 1000)

    val high = (1 to pulseWidth).map(_ => 1.0d).toList
    val low = (1 to pulseWidth).map(_ => -1.0d).toList

    // 1 second at 100 hz
    val sec = high ++ low

    assert(sec.length == 2000)
    val data: List[Double] = (1 to 600).flatMap(_ => sec).toList

    val dataSource = Source(data)

    def resample(
      durationMicroseconds: Long,
      sampleTimePeriodMicroseconds: Long
    ): Seq[(Double, Double)] = {
      val f = BaseTimeSeriesQuery.performStreamResampling(
        true,
        durationMicroseconds,
        sampleTimePeriodMicroseconds,
        sampleRate,
        dataSource
      )
      val (data, size @ _) = Await.result(f, 10 seconds)

      val minMax: Vector[(Double, Double)] =
        data.grouped(2).map(l => (l(0), l(1))).toVector
      minMax
    }

    val result = resample(23000000, 166646).toList

    val s = sliceEqual(result).map(_.length)

    //note last value is a remainder because request is cut off
    assert(s == List(30, 1, 29, 1, 29, 1, 30, 1, 17))

  }

}
