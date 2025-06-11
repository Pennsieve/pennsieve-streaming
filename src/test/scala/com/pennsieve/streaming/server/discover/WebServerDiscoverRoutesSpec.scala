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

package com.pennsieve.streaming.server.discover

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.Unauthorized
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.model.ws.{ BinaryMessage, TextMessage }
import akka.http.scaladsl.testkit.{ RouteTestTimeout, ScalatestRouteTest, WSProbe }
import akka.testkit.TestDuration
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.auth.middleware.Jwt.Role.RoleIdentifier
import com.pennsieve.auth.middleware.{ DatasetId, Jwt, OrganizationId, UserClaim, UserId }
import com.pennsieve.core.utilities.JwtAuthenticator._
import com.pennsieve.models.Role
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.clients.{ HttpError, MockDiscoverApiClient }
import com.pennsieve.streaming.query.{ LocalFilesystemWsClient, WsClient }
import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.TimeSeriesException.{ DiscoverApiError, NotTimeSeries }
import com.pennsieve.streaming.server._
import com.pennsieve.streaming.{ SessionGenerator, TestConfig, TestDatabase, TimeSeriesMessage }
import org.scalatest.{ fixture, BeforeAndAfter, Inspectors, Matchers }
import shapeless.syntax.inject._
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.util.Random

class WebServerDiscoverRoutesSpec
    extends fixture.WordSpec
    with Matchers
    with Inspectors
    with ScalatestRouteTest
    with TestDatabase
    with TestConfig
    with SessionGenerator
    with TSJsonSupport
    with BeforeAndAfter {
  implicit val log: ContextLogger = new ContextLogger()
  implicit val wsClient: WsClient = new LocalFilesystemWsClient
  implicit val ports: TestWebServerPorts = new TestWebServerPorts
  implicit val jwtConfig: Jwt.Config = new Jwt.Config {
    override def key: String = config.getString("jwt-key")
  }
  implicit def default(implicit system: ActorSystem): RouteTestTimeout =
    RouteTestTimeout(new DurationInt(3).second.dilated(system))

  private val userId = 1
  private val organizationId = 1
  private val datasetId = 1
  private val organizationRole: Jwt.Role = Jwt.OrganizationRole(
    OrganizationId(organizationId)
      .inject[RoleIdentifier[OrganizationId]],
    Role.Owner
  )
  private val datasetRole: Jwt.Role =
    Jwt.DatasetRole(DatasetId(datasetId).inject[RoleIdentifier[DatasetId]], Role.Owner)

  private val ownerClaim: Claim =
    Jwt.generateClaim(UserClaim(UserId(userId), List(organizationRole, datasetRole)), 1 minute)

  private val ownerToken = Jwt.generateToken(ownerClaim)

  after {
    ports.getDiscoverApiClient().asInstanceOf[MockDiscoverApiClient].resetResponse()
  }

  "montage validation route" should {
    "validate a montage that contains all correct channels" in { implicit dbSession =>
      val packageId = ports.MontagePackage
      val tokenHeader = OAuth2BearerToken(ownerToken.value)
      ports
        .getDiscoverApiClient()
        .asInstanceOf[MockDiscoverApiClient]
        .setResponse(Right(3))

      Get(s"/discover/ts/validate-montage?package=$packageId") ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        status should be(StatusCodes.OK)
      }
    }

    "invalidate a montage that is missing required channels" in { implicit dbSession =>
      val packageId = ports.InvalidMontagePackage
      val tokenHeader = OAuth2BearerToken(ownerToken.value)
      Get(s"/discover/ts/validate-montage?package=$packageId") ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        status should be(StatusCodes.BadRequest)
        responseAs[TimeSeriesException] should be(
          TimeSeriesException.UnexpectedError(
            "This package is missing channels that are required for all montages",
            Montage.allMontageChannelNames.toList
          )
        )
      }
    }

    "return the correct error if the package is not a time series" in { implicit dbSession =>
      val packageId = "not-a-time-series"
      val tokenHeader = OAuth2BearerToken(ownerToken.value)
      ports
        .getDiscoverApiClient()
        .asInstanceOf[MockDiscoverApiClient]
        .setResponse(Left(NotTimeSeries(packageId)))

      Get(s"/discover/ts/validate-montage?package=$packageId") ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        status should be(StatusCodes.BadRequest)
        responseAs[TimeSeriesException] should be(
          TimeSeriesException.UnexpectedError("not-a-time-series is not a time series package", Nil)
        )
      }
    }

    "return the correct error if the package is not in the discover database" in {
      implicit dbSession =>
        val packageId = "not-in-discover"
        val tokenHeader = OAuth2BearerToken(ownerToken.value)
        val underlyingHttpError = HttpError(StatusCodes.NotFound, s"Not found: ($packageId)")
        ports
          .getDiscoverApiClient()
          .asInstanceOf[MockDiscoverApiClient]
          .setResponse(Left(DiscoverApiError(underlyingHttpError)))

        Get(s"/discover/ts/validate-montage?package=$packageId") ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
          status should be(underlyingHttpError.statusCode)
          responseAs[TimeSeriesException] should be(
            TimeSeriesException.UnexpectedError(underlyingHttpError.getMessage, Nil)
          )
        }
    }

  }

  "timeseries flow route" should {
    "return a data flow for the requested channels" in { implicit dbSession =>
      val packageId = ports.InvalidMontagePackage

      val testClient = WSProbe()

      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$packageId"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        val timeSeriesRequest = createMessage(packageId)

        testClient.sendMessage(timeSeriesRequest)

        // two messages should be returned, one for each channel. They
        // could come in any order.
        val message1 = testClient.expectMessage()
        val message2 = testClient.expectMessage()

        val parsed = List(message1, message2).map {
          case BinaryMessage.Strict(bytes) =>
            val msg = TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array)
            val segment = msg.segment.get
            Right((segment.source, segment.channelName))
          case TextMessage.Strict(message) => Left(message)
          case _ => Left(())
        }
        parsed should contain theSameElementsAs ports.InvalidMontageMap.toList
          .map(Right.apply)
      }
    }

    "return a data flow starting at 0 for the requested channels if startAtEpoch is set" in {
      implicit dbSession =>
        val packageId = ports.GenericPackage

        val testClient = WSProbe()

        val tokenHeader = OAuth2BearerToken(ownerToken.value)

        val url = s"/discover/ts/query?package=$packageId&startAtEpoch=true"
        WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
          isWebSocketUpgrade shouldEqual true

          val timeSeriesRequest = TextMessage(
            TimeSeriesRequest(
              packageId = packageId,
              virtualChannels = Some(
                ports.GenericMap
                  .filter { case (_, name) => name contains "continuous" }
                  .map {
                    case (id, name) => VirtualChannel(id = id, name = name)
                  }
                  .toList
              ),
              startTime = 0,
              endTime = 350000000,
              pixelWidth = 1
            ).toJson.toString
          )

          testClient.sendMessage(timeSeriesRequest)

          // four messages should be returned, two for each channel. They
          // could come in any order.
          val messages = Range(0, 4).map(_ => testClient.expectMessage())

          val parsed = messages.map {
            case BinaryMessage.Strict(bytes) =>
              val msg = TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array)
              val segment = msg.segment.get
              Right(
                (
                  segment.source,
                  segment.channelName,
                  segment.startTs,
                  segment.pageStart,
                  segment.pageEnd
                )
              )
            case TextMessage.Strict(message) => Left(message)
            case _ => Left(())
          }

          val expected = List(
            ("paginated_continuous_ch1_id", "paginated_continuous_ch1", 0, 0, 350000000),
            ("paginated_continuous_ch1_id", "paginated_continuous_ch1", 200000000, 0, 350000000),
            ("paginated_continuous_ch2_id", "paginated_continuous_ch2", 100000000, 0, 350000000),
            ("paginated_continuous_ch2_id", "paginated_continuous_ch2", 300000000, 0, 350000000)
          ).map(Right.apply)

          parsed should contain theSameElementsAs expected
        }
    }

    "apply requested filters" in { implicit dbSession =>
      val packageId = ports.GenericPackage
      val MAX_FREQ = 1.0

      val testClient = WSProbe()

      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$packageId&startAtEpoch=true"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        val channel = ports.GenericMap
          .filter { case (_, name) => name contains "continuous" }
          .map {
            case (id, name) => VirtualChannel(id = id, name = name)
          }
          .head

        val timeSeriesRequest = TextMessage(
          TimeSeriesRequest(
            packageId = packageId,
            virtualChannels = Some(List(channel)),
            startTime = 0,
            endTime = 350000000,
            pixelWidth = 1
          ).toJson.toString
        )

        val filterRequest =
          TextMessage(FilterRequest("lowpass", List(4, MAX_FREQ), List(channel.id)).toJson.toString)

        testClient.sendMessage(timeSeriesRequest)

        // two messages should be returned for the one requested channel
        Range(0, 2).map(_ => testClient.expectMessage())

        testClient.sendMessage(filterRequest)
        testClient.expectNoMessage()

        testClient.sendMessage(timeSeriesRequest)

        // four more messages should be returned
        val messages = Range(0, 2).map(_ => testClient.expectMessage())

        val parsed = messages.map {
          case BinaryMessage.Strict(bytes) =>
            val msg = TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array)
            val segment = msg.segment.get
            Right(
              (
                segment.data,
                (
                  segment.source,
                  segment.channelName,
                  segment.startTs,
                  segment.pageStart,
                  segment.pageEnd
                )
              )
            )
          case TextMessage.Strict(message) => Left(message)
          case _ => Left(())
        }

        val data = parsed.map(_.right.get._1)
        val metadata = parsed.map(_.right.get._2)

        val expected = List(
          ("paginated_continuous_ch1_id", "paginated_continuous_ch1", 0, 0, 350000000),
          ("paginated_continuous_ch1_id", "paginated_continuous_ch1", 200000000, 0, 350000000)
        )

        metadata should contain theSameElementsAs expected
//        all(data.head.map(Math.abs)) should be < MAX_FREQ
      }
    }

    "support the old 'channels' key for backwards compatibility" in { implicit dbSession =>
      val packageId = ports.InvalidMontagePackage

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$packageId"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        val timeSeriesRequest = TextMessage(
          TimeSeriesRequest(
            packageId = packageId,
            channels = Some(ports.InvalidMontageIds),
            startTime = 0,
            endTime = 100,
            pixelWidth = 1
          ).toJson.toString
        )

        testClient.sendMessage(timeSeriesRequest)

        // two messages should be returned, one for each channel. They
        // could come in any order.
        val message1 = testClient.expectMessage()
        val message2 = testClient.expectMessage()

        val parsed = List(message1, message2).map {
          case BinaryMessage.Strict(bytes) =>
            val msg = TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array)
            val segment = msg.segment.get
            Right((segment.source, segment.channelName))
          case _ => Left(())
        }
        parsed should contain theSameElementsAs ports.InvalidMontageMap.toList
          .map(Right.apply)
      }
    }

    "return a montaged data flow when a montage has been applied" in { implicit dbSession =>
      val montagedPackage = ports.MontagePackage

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$montagedPackage"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        // a request to instruct the server to apply a montage
        val montageRequest = TextMessage(
          MontageRequest(packageId = ports.MontagePackage, montage = MontageType.ReferentialVsCz).toJson.toString
        )

        val virtualChannelsToRequest = Random
          .shuffle(MontageType.ReferentialVsCz.names.toList)
          .take(5)
          .map { virtualName =>
            val (leadChannel, _) =
              Montage
                .getMontagePair(virtualName, MontageType.ReferentialVsCz)
                .right
                .get
            val (virtualId, _) = ports.MontageMap.toList.find {
              case (_, name) => name == leadChannel
            }.get
            VirtualChannel(id = virtualId, name = virtualName)
          }

        val timeSeriesRequest = TextMessage(
          TimeSeriesRequest(
            packageId = montagedPackage,
            virtualChannels = Some(virtualChannelsToRequest),
            startTime = 0,
            endTime = 100,
            pixelWidth = 1
          ).toJson.toString
        )

        // send the montage request and expect the correct list of
        // virtual channels back
        testClient.sendMessage(montageRequest)
        val virtualChannelsList = ports
          .parseJsonFromMessage[ChannelsDetailsList](testClient.expectMessage())
          .channelDetails
          .map(vc => vc.id -> vc.name)

        val expected = MontageType.ReferentialVsCz.pairs.map {
          case (lead, secondary) =>
            s"${lead}_id" -> Montage.getMontageName(lead, secondary)
        }

        virtualChannelsList should contain theSameElementsAs expected

        // send the actual request for data, expecting to receive a
        // montaged data stream
        testClient.sendMessage(timeSeriesRequest)

        // A message should be returned for each montage pair. These
        // messages could come in any order.
        val montagedChannels =
          for (_ <- virtualChannelsToRequest.indices)
            yield {
              testClient.expectMessage() match {
                case TextMessage.Strict(error) =>
                  fail(
                    s"Error encountered from test ws connection: ${error.parseJson.convertTo[TimeSeriesError]}"
                  )
                case BinaryMessage.Strict(message) =>
                  TimeSeriesMessage.parseFrom(message.toArray)
                case _ => fail("Unexpected message type")
              }
            }

        val names = montagedChannels.map { msg =>
          val leadChannel =
            ports.MontageIds.find(_ == msg.segment.get.source).get
          val name = msg.segment.get.channelName

          val (leadName, secondaryName) =
            Montage
              .getMontagePair(name, MontageType.ReferentialVsCz)
              .right
              .get
          leadName should be(ports.MontageMap(leadChannel))

          Montage.getMontageName(leadName, secondaryName)
        }
        val ids = montagedChannels.map(_.segment.get.source)

        names should contain theSameElementsAs virtualChannelsToRequest.map(_.name)
        ids should contain theSameElementsAs virtualChannelsToRequest.map(_.id)
      }
    }

    "return the package channels list when the montage state is cleared" in { implicit dbSession =>
      val `package` = ports.MontagePackage

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=${`package`}"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        // a request to instruct the server to clear the montage state
        val montageRequest =
          TextMessage(
            MontageRequest(packageId = ports.MontagePackage, montage = MontageType.NotMontaged).toJson.toString
          )

        // send the montage request and expect the correct list of
        // virtual channels back
        testClient.sendMessage(montageRequest)
        val virtualChannelsList = ports
          .parseJsonFromMessage[ChannelsDetailsList](testClient.expectMessage())
          .channelDetails
          .map(vc => vc.id -> vc.name)
        val expected = ports.MontageMap.toList

        virtualChannelsList should contain theSameElementsAs expected
      }
    }

    "do nothing with a keepAlive message" in { implicit dbSession =>
      val `package` = ports.MontagePackage

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=${`package`}"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        val keepAlive =
          TextMessage(KeepAlive().toJson.toString)

        testClient.sendMessage(keepAlive)

        // no actual message will be sent
        testClient.inProbe.expectSubscription()
      }
    }

    "bubble up montage missing channels errors to the client" in { implicit dbSession =>
      val montagePackage = ports.InvalidMontagePackage

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$montagePackage"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        // a request to instruct the server to apply a montage
        val montageRequest = TextMessage(
          MontageRequest(
            packageId = ports.InvalidMontagePackage,
            montage = MontageType.ReferentialVsCz
          ).toJson.toString
        )

        // this should return an error because this package is not
        // eligible for this montage
        testClient.sendMessage(montageRequest)

        val tse = ports.parseJsonFromMessage[TimeSeriesError](testClient.expectMessage())

        tse.error should be("PackageCannotBeMontaged")
        tse.reason should be(
          """This package is missing channels that are required for the "REFERENTIAL_VS_CZ" montage"""
        )
        tse.channelNames should contain theSameElementsAs MontageType.ReferentialVsCz.distinctValues

      }
    }

    "bubble up missing channel errors to the client" in { implicit dbSession =>
      val montagePackage = ports.InvalidMontagePackage

      val testClient = WSProbe()

      val missingChannelsToRequest =
        (ports.MontageMap.toList.head :: ports.InvalidMontageMap.toList)
          .map {
            case (id, name) =>
              VirtualChannel(id = id, name = name)
          }
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$montagePackage"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        val timeSeriesRequest = TextMessage(
          TimeSeriesRequest(
            packageId = montagePackage,
            virtualChannels = Some(missingChannelsToRequest),
            startTime = 0,
            endTime = 100,
            pixelWidth = 1
          ).toJson.toString
        )

        testClient.sendMessage(timeSeriesRequest)
        val tse = ports.parseJsonFromMessage[TimeSeriesError](testClient.expectMessage())

        tse.error should be("PackageMissingChannels")
        tse.channelNames should be(List(ports.MontageIds.head))
      }
    }
  }

  "A websocket request sent with a jwt auth token" should {
    "stream back data as expected" in { implicit dbSession =>
      val packageId = ports.MontagePackage

      val testClient = WSProbe()

      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$packageId"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        isWebSocketUpgrade shouldEqual true

        val timeSeriesRequest = createMessage(packageId)

        testClient.sendMessage(timeSeriesRequest)

        // two messages should be returned, one for each channel. They
        // could come in any order.
        val message1 = testClient.expectMessage()
        val message2 = testClient.expectMessage()

        List(message1, message2).map {
          case BinaryMessage.Strict(bytes) =>
            val msg = TimeSeriesMessage.parseFrom(bytes.toByteBuffer.array)
            val segment = msg.segment.get
            Right((segment.source, segment.channelName))
          case _ => Left(())
        }
      }
    }

    "return unauthorized for a service level token" in { implicit dbSession =>
      val packageId = ports.MontagePackage

      val testClient = WSProbe()

      implicit val jwtConfig: Jwt.Config = new Jwt.Config {
        override def key: String = config.getString("jwt-key")
      }

      val token =
        generateServiceToken(1 minute, organizationId, Some(datasetId))

      val tokenHeader = OAuth2BearerToken(token.value)

      val url = s"/discover/ts/query?package=$packageId"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        status shouldBe Unauthorized
      }
    }

    "return bad request for a non-time series package" in { implicit dbSession =>
      val packageId = "not-a-time-series"
      ports
        .getDiscoverApiClient()
        .asInstanceOf[MockDiscoverApiClient]
        .setResponse(Left(NotTimeSeries(packageId)))

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$packageId"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[TimeSeriesException] should be(
          TimeSeriesException.UnexpectedError("not-a-time-series is not a time series package", Nil)
        )

      }
    }

    "return not found for a package not in the discover database" in { implicit dbSession =>
      val packageId = "not-in-discover"
      val underlyingHttpError = HttpError(StatusCodes.NotFound, s"Not found: ($packageId)")
      ports
        .getDiscoverApiClient()
        .asInstanceOf[MockDiscoverApiClient]
        .setResponse(Left(DiscoverApiError(underlyingHttpError)))

      val testClient = WSProbe()
      val tokenHeader = OAuth2BearerToken(ownerToken.value)

      val url = s"/discover/ts/query?package=$packageId"
      WS(url, testClient.flow) ~> Authorization(tokenHeader) ~> new WebServer().route ~> check {
        status shouldBe StatusCodes.NotFound
        responseAs[TimeSeriesException] should be(
          TimeSeriesException.UnexpectedError(underlyingHttpError.getMessage, Nil)
        )

      }
    }

  }

  private def createMessage(packageId: String) =
    TextMessage(
      TimeSeriesRequest(packageId = packageId, virtualChannels = Some(ports.InvalidMontageMap.map {
        case (id, name) => VirtualChannel(id = id, name = name)
      }.toList), startTime = 0, endTime = 100, pixelWidth = 1).toJson.toString
    )
}
