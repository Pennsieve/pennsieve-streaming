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
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
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

  var fakeConfig: com.typesafe.config.Config = _

  var wsClient: S3WsClient = _
  val url = "/test-data"

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

    bindingFuture = Http().bindAndHandle(testRoute, "localhost", 0)
    val port = Await.result(bindingFuture, 3.seconds).localAddress.getPort
    fakeConfig = ConfigFactory
      .empty()
      .withValue("timeseries.s3-use-ssl", ConfigValueFactory.fromAnyRef(false))
      .withValue("timeseries.s3-host", ConfigValueFactory.fromAnyRef("localhost"))
      .withValue("timeseries.s3-port", ConfigValueFactory.fromAnyRef(port))
      .withValue("timeseries.request-queue-size", ConfigValueFactory.fromAnyRef(10))
      .withValue("timeseries.max-message-queue", ConfigValueFactory.fromAnyRef(3))
    wsClient = new S3WsClient(fakeConfig)
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

    println(s"got stream with ${result.length} items")
    result should not be empty
    println(s"Streamed ${result.length} values from $url")
  }

  "getEventSource" should "not fail with TimeoutException after a delay" in {

    val sourceFut = wsClient.getEventSource(url)

    Thread.sleep(2000)

    val dataFut = sourceFut.flatMap(_.runWith(Sink.seq))
    val result = Await.result(dataFut, 5.seconds)

    println(s"got stream with ${result.length} items")
    result should not be empty
    println(s"Streamed ${result.length} values from $url")
  }

}
