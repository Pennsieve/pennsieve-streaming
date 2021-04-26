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

import akka.http.scaladsl.model.{ StatusCode, StatusCodes }
import com.pennsieve.domain.CoreError
import com.pennsieve.streaming.server.TSJsonSupport._
import spray.json._

sealed trait TimeSeriesException extends Exception {
  val name: String
  val reason: String

  // An optional list of channel names that are applicable in the
  // context of this error
  val channelNames: List[String] = List.empty

  val statusCode: StatusCode

  override def toString = s"$name: $reason: $channelNames"

  def json: String =
    TimeSeriesError(error = name, reason = reason, channelNames = channelNames).toJson.toString
}

object TimeSeriesException {

  /** Channels existed in the request but were not present in the
    * package
    */
  case class PackageMissingChannels(override val channelNames: List[String], packageId: String)
      extends TimeSeriesException {
    override val name = "PackageMissingChannels"
    override val reason =
      s"Some of the requested channels do not exist in this package: $packageId"

    override val statusCode = StatusCodes.NotFound
  }

  /** The request was missing both the virtualChannels and the
    * channels keys
    */
  case object RequestMissingChannels extends TimeSeriesException {
    override val name = "RequestMissingChannels"
    override val reason =
      "The request did not have a 'channels' or a 'virtualChannels' key"

    override val statusCode = StatusCodes.BadRequest
  }

  /** A montage name contained too many Montage.SEPARATOR characters */
  case class InvalidMontageName(montageName: String) extends TimeSeriesException {
    override val name = "InvalidMontageName"
    override val reason =
      s"Invalid montage name: $montageName"

    override val statusCode = StatusCodes.BadRequest
  }

  /** The lead and secondary channel data was not split up amongst
    * different files in s3 in the same manner
    */
  case class MontageMisalignment(leadChannelName: String, secondaryChannelName: String)
      extends TimeSeriesException {
    override val name = "MontageMisalignment"
    override val reason =
      "The lead channel did not correspond to the same file structure as the secondary channel"
    override val channelNames = List(leadChannelName, secondaryChannelName)

    override val statusCode = StatusCodes.BadRequest
  }

  /** The package could not be montaged because it is missing some of
    * the required channels
    */
  case class PackageCannotBeMontaged(missingChannelNames: List[String], montageType: MontageType)
      extends TimeSeriesException {
    override val name = "PackageCannotBeMontaged"

    val montageName = montageType match {
      case MontageType.NotMontaged => "all montages"
      case mt => s"the ${mt.toJson} montage"
    }

    override val reason =
      s"This package is missing channels that are required for $montageName"

    override val channelNames = missingChannelNames

    override val statusCode = StatusCodes.BadRequest
  }

  /** Error originating in the pennsieve api */
  case class PennsieveApiError(error: CoreError) extends TimeSeriesException {
    override val name = "PennsieveApiError"
    override val reason = error.toString

    override val statusCode = StatusCodes.InternalServerError
  }

  def fromCoreError(coreError: CoreError): TimeSeriesException = {
    PennsieveApiError(coreError)
  }

  /** Catch-all for unexpected errors */
  case class UnexpectedError(
    override val reason: String,
    override val channelNames: List[String] = List.empty
  ) extends TimeSeriesException {
    override val name = "UnexpectedError"

    override val statusCode = StatusCodes.InternalServerError
  }
}
