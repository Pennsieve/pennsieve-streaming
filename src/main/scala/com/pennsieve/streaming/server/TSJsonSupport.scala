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

package com.pennsieve.streaming.server

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.pennsieve.models.Channel
import com.pennsieve.streaming.query.{ RangeRequest, UnitRangeRequest }
import com.pennsieve.streaming.server.TimeSeriesFlow.WithError
import com.pennsieve.streaming.{ LookupResultRow, RangeLookUp, UnitRangeEntry, UnitRangeLookUp }
import scalikejdbc.DBSession
import spray.json.{ DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat }
import spray.json.DefaultJsonProtocol._

/** A list of virtual channels */
case class ChannelsList(virtualChannels: List[VirtualChannel])

/** A serializeable type for any error originating in this service */
case class TimeSeriesError(error: String, reason: String, channelNames: List[String])

/** A virtual representation of a single channel stream on the front
  * end. This might be a montaged channel, or it could just be a normal
  * single channel.
  */
final case class VirtualChannel(id: String, name: String)

/** A list of virtual channels */
case class ChannelsDetailsList(channelDetails: List[VirtualChannelInfo])

/**
  * Representation of channel detail which is send to the client in response to Montge request
  * @param id
  * @param name
  * @param start
  * @param end
  */
final case class VirtualChannelInfo(
  id: String,
  name: String,
  start: Long,
  end: Long,
  channelType: String,
  rate: Double,
  unit: String
)

/** Indicates that the request type will elicit a websocket response
  * from this service
  */
sealed trait Respondable

final case class TimeSeriesRequest(
  packageId: String,
  startTime: Long,
  endTime: Long,
  pixelWidth: Long,
  channels: Option[List[String]] = None,
  virtualChannels: Option[List[VirtualChannel]] = None,
  requestAnnotations: Option[Boolean] = Some(false),
  queryLimit: Option[Long] = None
) extends Respondable {

  /** Get the ids of all channels in the request */
  def channelIds: WithError[List[String]] =
    virtualChannels match {
      case Some(vc) => Right(vc.map(_.id))
      case None =>
        channels match {
          case Some(chans) => Right(chans)
          case None => Left(TimeSeriesException.RequestMissingChannels)
        }
    }

  /** Given a channelMap containing all channels in this request, get
    * a list of virtualChannels in this request even if the
    * virtualChannels list is empty. This is used for backwards
    * compatibility in case the front end is still using the old
    * request format.
    */
  def getVirtualChannels(channelMap: Map[String, Channel]): WithError[List[VirtualChannel]] =
    virtualChannels match {
      case Some(vc) => Right(vc)
      case None =>
        channels match {
          case Some(chans) =>
            Right(chans.map(c => VirtualChannel(c, channelMap(c).name)))
          case None => Left(TimeSeriesException.RequestMissingChannels)
        }
    }

  /** Enumerate the given list with the given `numberit` function */
  private def numberSequentially[T](ls: Seq[T])(numberit: ((T, Int)) => T): Seq[T] =
    ls.zipWithIndex.map(numberit)

  /** Get the RangeRequest objects that correspond to this object and
    * the given channel
    *
    * A single channel can have multiple RangeRequests because a
    * single channel is stored in multiple files in S3. Depending on
    * the given range in the request, we might need to grab data from
    * multiple files.
    *
    * @param lookup An object that is used to look up the given channel
    *        data in the database
    * @param channel The channel for which to fetch the data described
    *        in the request
    *
    * @return A sequence of RangeRequest objects corresponding to different
    *         locations in s3 that contain the requested data.
    */
  def toContinuousRangeRequests(
    lookup: RangeLookUp
  )(
    channel: Channel
  )(implicit
    session: DBSession
  ): Seq[RangeRequest] = {
    val lkup: Seq[LookupResultRow] =
      lookup.lookup(startTime, endTime, channel.nodeId)

    // First check the database to find the requested range for this
    // channel
    val requests: Seq[RangeRequest] = lkup map { lookupresult =>
      val lim = queryLimit map (_.toInt)
      RangeRequest(channel.nodeId, channel.name, startTime, endTime, lim, pixelWidth, lookupresult)
    }

    // Enumerate the data for this range
    val numberedRequests = numberSequentially(requests) {
      case (r, indx) =>
        r.copy(sequenceId = Some(indx), totalRequests = Some(requests.length))
    }

    // If there was no corresponding data in the database, just use
    // the given channel to create a RangeRequest
    if (numberedRequests.isEmpty) {
      val emptyLookup = LookupResultRow(-1, startTime, endTime, channel.rate, channel.nodeId, "")
      List(
        RangeRequest(
          channel.nodeId,
          channel.name,
          startTime,
          endTime,
          Some(0),
          pixelWidth,
          emptyLookup
        )
      )
    } else {
      numberedRequests
    }
  }

  /** Get the UnitRangeRequest objects that correspond to this object
    * and the given channel
    *
    * @param lookup An object that is used to look up the given channel
    *        data in the database
    * @param channel The channel for which to fetch the data described
    *        in the request
    *
    * @return A sequence of UnitRangeRequest objects corresponding to different
    *         locations in s3 that contain the requested data.
    */
  def toUnitRangeRequests(
    lookup: UnitRangeLookUp
  )(
    channel: Channel
  )(implicit
    session: DBSession
  ): Seq[UnitRangeRequest] = {
    val spikeDuration: Long = channel.spikeDuration.getOrElse(0)
    val spikeCount = Math.ceil((spikeDuration * channel.rate) / 1e6)

    val requests = lookup
      .lookup(startTime, endTime, channel.nodeId) map { entry =>
      UnitRangeRequest(
        channel.nodeId,
        startTime,
        endTime,
        queryLimit,
        pixelWidth,
        spikeDuration,
        spikeCount.toInt,
        channel.rate,
        entry
      )
    }

    val numbered = numberSequentially(requests) {
      case (r, indx) =>
        r.copy(sequenceId = Some(indx), totalRequests = Some(requests.length))
    }

    if (numbered.isEmpty) {
      val emptyLookup =
        UnitRangeEntry(-1, 0, 0, channel.nodeId, 0, "", "")
      List(
        UnitRangeRequest(
          channel.nodeId,
          startTime,
          endTime,
          Some(0),
          pixelWidth,
          spikeDuration,
          spikeCount.toInt,
          channel.rate,
          emptyLookup
        )
      )
    } else numbered
  }
}

final case class FilterRequest(
  filter: String,
  filterParameters: List[Double],
  channels: List[String]
)

final case class MontageRequest(
  packageId: String,
  montage: MontageType,
  montageMap: Option[List[(String, String)]] = None
) extends Respondable

final case class ClearFilterRequest(channelFiltersToClear: List[String])
final case class ResetFilterRequest(channelFiltersToReset: List[String])
final case class KeepAlive(currentTime: Long = System.currentTimeMillis())
final case class HealthCheck(connections: Long, age: Long, currentTime: Long)
final case class DumpBufferRequest(requestType: String = "DumpBufferRequest") extends Respondable
case class TimestampedRequest(original: Respondable, timestamp: Long) extends Respondable

trait TSJsonSupport extends SprayJsonSupport with DefaultJsonProtocol
object TSJsonSupport {
  implicit object MontageTypeFormat extends RootJsonFormat[MontageType] {
    def write(c: MontageType) = {
      val toString = c match {
        case MontageType.NotMontaged => "NOT_MONTAGED"
        case MontageType.BipolarAntPos => "BIPOLAR_ANT_POS"
        case MontageType.BipolarTransverse => "BIPOLAR_TRANSVERSE"
        case MontageType.ReferentialVsCz => "REFERENTIAL_VS_CZ"
        case MontageType.CustomMontage() => "CUSTOM_MONTAGE"
      }
      JsString(toString)
    }

    def read(value: JsValue) = value match {
      case JsString(montageType) =>
        if (montageType == "NOT_MONTAGED") MontageType.NotMontaged
        else if (montageType == "BIPOLAR_ANT_POS") MontageType.BipolarAntPos
        else if (montageType == "BIPOLAR_TRANSVERSE")
          MontageType.BipolarTransverse
        else if (montageType == "REFERENTIAL_VS_CZ") MontageType.ReferentialVsCz
        else if (montageType == "CUSTOM_MONTAGE") MontageType.CustomMontage()
        else throw new Exception(s"Invalid value for MontageType: $montageType")
      case _ => throw new Exception("Invalid json format")
    }
  }

  implicit object TimeSeriesExceptionFormat extends RootJsonFormat[TimeSeriesException] {
    def write(c: TimeSeriesException) =
      JsObject(
        "error" -> JsString(c.name),
        "reason" -> JsString(c.reason),
        "channelNames" -> JsArray(c.channelNames.map(JsString.apply).toVector)
      )

    def read(value: JsValue) = value match {
      case JsObject(fieldMap)
          if fieldMap.contains("reason") && fieldMap
            .contains("channelNames") => {
        val reason = fieldMap("reason") match {
          case JsString(r) => r
          case _ => throw new Exception("Invalid json format")
        }
        val channels: Vector[String] = fieldMap("channelNames") match {
          case JsArray(chans) =>
            chans.map {
              case JsString(chan) => chan
              case _ => throw new Exception("Invalid json format")
            }
          case _ => throw new Exception("Invalid json format")
        }
        TimeSeriesException.UnexpectedError(reason, channels.toList)
      }
      case _ => throw new Exception("Invalid json format")
    }
  }

  implicit val minimalChannelFormat = jsonFormat2(VirtualChannel)
  implicit val channelsListFormat = jsonFormat1(ChannelsList)
  implicit val detailsChannelFormat = jsonFormat7(VirtualChannelInfo)
  implicit val channelsDetailsListFormat = jsonFormat1(ChannelsDetailsList)
  implicit val timeSeriesErrorFormat = jsonFormat3(TimeSeriesError)
  implicit val timeSeriesRequestFormat = jsonFormat8(TimeSeriesRequest)
  implicit val KeepAliveFormat = jsonFormat1(KeepAlive)
  implicit val FilterRequestFormat = jsonFormat3(FilterRequest)
  implicit val MontageRequestFormat = jsonFormat3(MontageRequest)
  implicit val ClearFilterRequestFormat = jsonFormat1(ClearFilterRequest)
  implicit val ResetFilterRequestFormat = jsonFormat1(ResetFilterRequest)
  implicit val lookupResultRowFormat = jsonFormat6(LookupResultRow)
  implicit val dumpBufferRequestFormat = jsonFormat1(DumpBufferRequest)
  implicit val health = jsonFormat3(HealthCheck)
}
