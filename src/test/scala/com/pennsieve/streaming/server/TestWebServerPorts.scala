package com.pennsieve.streaming.server

import akka.http.scaladsl.model.ws.Message
import cats.data.EitherT
import cats.instances.future._
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.models.Channel
import com.pennsieve.streaming.TimeSeriesLogContext
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT

import scala.concurrent.ExecutionContext.Implicits.global
import spray.json._

class TestWebServerPorts extends WebServerPorts {
  /*
   * A generic continuous package with four channels - two continuous
   * and one unit - that correspond to actual data on the filesystem
   * that can be streamed via the LocalFilesytemWsClient in tests.
   */
  val GenericPackage = "generic_package_id"
  val GenericNames = List(
    "paginated_continuous_ch1",
    "paginated_continuous_ch2",
    "paginated_unit_ch1",
    "paginated_unit_ch2"
  )
  val GenericIds = GenericNames map getId
  val GenericMap = GenericIds.zip(GenericNames).toMap
  val GenericChannelMap =
    GenericIds
      .map(
        id =>
          id -> Channel(
            nodeId = id,
            packageId = 0,
            name = GenericMap(id),
            start =
              if (id.endsWith("ch1_id")) 200000000
              else 300000000,
            end =
              if (id.endsWith("ch1_id")) 600000000
              else 700000000,
            unit = "",
            rate = 100,
            `type` = if (id.contains("unit")) "unit" else "continuous",
            group = None,
            lastAnnotation = 0,
            spikeDuration = Some(100)
          )
      )
      .toMap

  /*
   * A package that cannot be montaged
   */
  val InvalidMontagePackage = "invalid_montage_package_id"
  val InvalidMontageNames = List("channel1", "channel2")
  val InvalidMontageIds = InvalidMontageNames map getId
  val InvalidMontageMap = InvalidMontageNames
    .map(name => getId(name) -> name)
    .toMap

  /*
   * A package with a channel that doesn't exist in the database
   */
  val NonexistentChannelPackage = "nonexistent_package_id"
  val NonexistentChannelNames = List("nonexistent_channel")
  val NonexistentChannelIds = NonexistentChannelNames map getId
  val NonexistentChannelMap = NonexistentChannelNames
    .map(name => getId(name) -> name)
    .toMap

  /*
   * A package that is compatible with all montages
   */
  val MontagePackage = "montage_package_id"
  val MontageNames = Montage.allMontageChannelNames.toList
  val MontageIds = MontageNames map getId
  val MontageMap = MontageNames
    .map(name => getId(name) -> name)
    .toMap

  /*
   * A map of channelIds that are used in tests to what we expect them
   * to be named. This is required by our naive implementation of
   * getChannelByNodeId below.
   */
  val channelMap = InvalidMontageMap ++ NonexistentChannelMap ++ MontageMap ++ GenericMap

  /*
   * A map of packageIds to the channels we expect to be contained in
   * those packages
   */
  val packageMap = Map(
    MontagePackage -> MontageIds
      .map(createDummyChannel),
    NonexistentChannelPackage -> NonexistentChannelIds
      .map(createDummyChannel),
    InvalidMontagePackage -> InvalidMontageIds
      .map(createDummyChannel),
    GenericPackage -> GenericIds
      .map(GenericChannelMap)
  )

  private def getId(channelName: String) = channelName + "_id"

  def createDummyChannel(channelId: String): Channel =
    Channel(
      nodeId = channelId,
      packageId = 0,
      name = channelMap(channelId),
      start = 0,
      end = 0,
      unit = "",
      rate = 0.0,
      `type` = "continuous",
      group = None,
      lastAnnotation = 0
    )

  override def getChannels(
    packageNodeId: String,
    claim: Claim
  ): WithErrorT[(List[Channel], TimeSeriesLogContext)] =
    EitherT.pure((packageMap(packageNodeId), TimeSeriesLogContext()))

  override def getChannelByNodeId(
    channelNodeId: String,
    claim: Claim
  ): WithErrorT[(Channel, TimeSeriesLogContext)] =
    EitherT.pure((createDummyChannel(channelNodeId), TimeSeriesLogContext()))

  override val rangeLookupQuery =
    "select id, location, channel, rate, hi, lo from timeseries.ranges where (channel = {channel}) and ((lo >= {qstart} and lo <= {qend}) or (hi >= {qstart} and hi <= {qend}) or (lo < {qstart} and hi > {qend}) ) order by lo asc"
  override val unitRangeLookupQuery =
    "select id, count, channel, tsindex, tsblob, lo, hi from timeseries.unit_ranges where (channel = {channel}) and ((lo >= {qstart} and lo <= {qend}) or (hi >= {qstart} and hi <= {qend}) or (lo < {qstart} and hi > {qend})  ) order by lo asc "

  /** Parse the given json-serializeable type from the given websocket
    * message
    */
  def parseJsonFromMessage[A](message: Message)(implicit e: JsonReader[A]): A =
    message.asTextMessage.getStrictText.parseJson.convertTo[A]

  /** Parse the given websocket message into the given protobuf type
    * using the given parse function
    */
  def parseProtobufFromMessage[A](parse: Array[Byte] => A)(message: Message): A =
    parse(message.asBinaryMessage.getStrictData.toArray)
}
