package com.pennsieve.streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{ `Content-Encoding`, HttpEncodings }
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.{ S3WsClient, WsClient }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import com.pennsieve.streaming.query.LocalFilesystemWsClient

/**
  * Tests the S3WsClient behavior when retrieving a gzipped stream.
  * Simulates delayed consumption to verify that the stream is correctly materialized.
  */
class WsClientSpec
    extends FlatSpec
    with Matchers
    with AkkaImplicits
    with BeforeAndAfterAll
    with TestConfig {

  implicit val log: ContextLogger = new ContextLogger()
  implicit val mat: Materializer = Materializer(system)

  private var bindingFuture: Future[Http.ServerBinding] = _
  private val port = 8081

  val fakeConfig = ConfigFactory.parseString("""
        timeseries.s3-host = "localhost"
        timeseries.s3-port = 8081
        timeseries.request-queue-size = 10
    """)

  val wsClient = new S3WsClient(fakeConfig)
  val url = s"http://localhost:8081/test-data"

  override def beforeAll(): Unit = {
    val testData = (1 to 100000).map(_.toString)
    val gzippedContent = gzippedBytes(testData)

    val testRoute =
      path("test-data") {
        get {
          complete(
            HttpResponse(
              entity = HttpEntity(ContentTypes.`application/octet-stream`, gzippedContent)
            ).withHeaders(`Content-Encoding`(HttpEncodings.gzip))
          )
        }
      }

    bindingFuture = Http().bindAndHandle(testRoute, "localhost", port)
    Await.result(bindingFuture, 3.seconds)
  }

  override def afterAll(): Unit = {
    Await.result(bindingFuture.flatMap(_.unbind()), 3.seconds)
    Await.result(system.terminate(), 5.seconds)
  }

  private def gzippedBytes(lines: Seq[String]): ByteString = {
    val out = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(out)
    lines.foreach(line => gzip.write((line + "\n").getBytes("UTF-8")))
    gzip.close()
    ByteString(out.toByteArray)
  }

  "getDataSource" should "not fail with TimeoutException after a delay" in {

    val sourceFut = wsClient.getDataSource(url)

    Thread.sleep(2000)

    val dataFut = sourceFut.flatMap(_.runWith(Sink.seq))
    val result = Await.result(dataFut, 5.seconds)

    println(s"got stream with ${result.length} items: $result")
    result should not be empty
    println(s"Streamed ${result.length} values from $url")
  }

  "getEventSource" should "not fail with TimeoutException after a delay" in {

    val sourceFut = wsClient.getEventSource(url)

    Thread.sleep(2000)

    val dataFut = sourceFut.flatMap(_.runWith(Sink.seq))
    val result = Await.result(dataFut, 5.seconds)

    println(s"got stream with ${result.length} items: $result")
    result should not be empty
    println(s"Streamed ${result.length} values from $url")
  }

}
