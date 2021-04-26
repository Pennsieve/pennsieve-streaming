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

import cats.implicits._
import com.pennsieve.models.Channel
import com.pennsieve.streaming.server.TimeSeriesFlow.WithError
import scala.collection.concurrent

/** Utility functions and constants for working with montages */
object Montage {

  // The separator between channel names for all montages
  private val SEPARATOR = '-'

  /** A complete list of all channels required for */
  def allMontageChannelNames: Set[String] =
    MontageType.ReferentialVsCz.distinctValues | MontageType.BipolarAntPos.distinctValues | MontageType.BipolarTransverse.distinctValues

  /** Validate that the given list of channel names can be fully
    * montaged with the given montageType. If montageType is None,
    * then no montage is active and any configuration of channelNames
    * is valid.
    */
  def validateMontage(
    channelNames: List[String],
    montageType: MontageType
  ): Either[TimeSeriesException, Unit] = montageType match {
    case MontageType.NotMontaged => Right(())
    case mt =>
      ensureAllChannelsArePresent(channelNames.toSet, mt.distinctValues, mt)
  }

  /** Validate that the given list of channel names can be fully
    * montaged with any montageType
    */
  def validateAllMontages(channelNames: List[String]): Either[TimeSeriesException, Unit] =
    ensureAllChannelsArePresent(channelNames.toSet, allMontageChannelNames, MontageType.NotMontaged)

  /** Ensure that the given set of channelNames is a superset of the
    * set of desiredChannelNames. The given montageType is used in the
    * error message if an error is found.
    *
    * If the montageType is None, it is assumed that the given
    * desiredChannelNames set represents all channels in all montages.
    */
  private def ensureAllChannelsArePresent(
    channelNames: Set[String],
    desiredChannelNames: Set[String],
    montageType: MontageType
  ): Either[TimeSeriesException, Unit] = {
    val missingChannels = desiredChannelNames -- channelNames

    if (missingChannels.isEmpty)
      Right(())
    else
      Left(
        TimeSeriesException
          .PackageCannotBeMontaged(missingChannels.toList, montageType)
      )
  }

  /** Confirm that the given channelMap can be montaged with the given
    * montageType
    *
    * @param montageType A montageType that will be applied to the
    *        channelMap
    * @param channelMap A map of channelNodeId -> Channel. This map
    *        generally represents all channels in a package.
    *
    * @return An either containing an error on the Left side if this
    *         package cannot be montaged, otherwise Unit on the Right
    *         side.
    */
  def checkMontageability(
    montageType: MontageType,
    channelMap: Map[String, Channel]
  ): WithError[Unit] = {
    val channelsMissingFromMontage = montageType match {
      case MontageType.NotMontaged => Set.empty
      case mt => {
        val channelsInMontage = mt.pairs
          .flatMap(tup => List(tup._1, tup._2))
          .toSet
        channelsInMontage -- channelMap.values.map(_.name).toSet
      }
    }

    if (channelsMissingFromMontage.isEmpty) Right(())
    else
      Left(
        TimeSeriesException
          .PackageCannotBeMontaged(channelsMissingFromMontage.toList, montageType)
      )
  }

  /** Build a list of leadChannels to their corresponding
    * secondaryChannels using the given montageType and channels
    * list. If the montageType is not defined, all secondaryChannels
    * will be None.
    *
    * @param montageType An optional montage type to apply.
    * @param packageId The packageId for the current request. This is used
    *        to include more information in the error if the channels
    *        provided in the request did not exist in the package.
    * @param channelMap A map of channelNodeId -> Channel. This map
    *        represents all channels in the package.
    * @param channels A list of virtual channels in this request.
    *
    * @return A list of channel pairs indicating which channels to
    *         combine when montaging in order to fulfill the request
    */
  def buildMontage(
    packageMontages: concurrent.Map[String, MontageType],
    packageId: String,
    channelMap: Map[String, Channel],
    channels: List[VirtualChannel]
  ): WithError[List[(Channel, Option[Channel])]] = {
    val montageType =
      packageMontages.getOrElse(packageId, MontageType.NotMontaged)
    for {
      pairs <- channels.map(c => getMontagePair(c.name, montageType)).sequence
      pairMap = pairs.map(p => getMontageName(p) -> p).toMap

      // Confirm that this package can be montaged - all channels in
      // the montage must exist in the package
      _ <- checkMontageability(montageType, channelMap)

      // Confirm that the request is valid - all channels in the
      // request must exist in the package
      channelsMissingFromPackage = {
        val channelsInRequest = pairs
          .flatMap(tup => tup._1 :: tup._2.toList)
          .toSet

        channelsInRequest -- channelMap.values.map(_.name).toSet
      }
      _ <- if (channelsMissingFromPackage.isEmpty) Right(())
      else
        Left(
          TimeSeriesException
            .PackageMissingChannels(channelsMissingFromPackage.toList, packageId)
        )

    } yield
      montageType match {
        case MontageType.NotMontaged =>
          channels.map(chan => (channelMap(chan.id), None))
        case _ => {
          channels.map { channel =>
            val (leadChannel, maybeSecondaryChannel) = pairMap(channel.name)
            val leadId =
              channelMap.values.find(_.name == leadChannel).get.nodeId
            val maybeSecondaryId = maybeSecondaryChannel.map(
              secondaryChannel =>
                channelMap.values
                  .find(_.name == secondaryChannel)
                  .get
                  .nodeId
            )

            channelMap(leadId) -> maybeSecondaryId.flatMap(channelMap.get)
          }
        }
      }
  }

  /** Given two channel names, get the name of their virtual montaged
    * channel:
    *
    *   <leadChannel>-<secondaryChannel>
    */
  def getMontageName(leadChannel: String, secondaryChannel: Option[String]): String =
    leadChannel + secondaryChannel.map(SEPARATOR + _).getOrElse("")

  def getMontageName(channelPair: (String, Option[String])): String =
    channelPair._1 + channelPair._2.map(SEPARATOR + _).getOrElse("")

  def getMontageName(leadChannel: String, secondaryChannel: String): String =
    leadChannel + SEPARATOR + secondaryChannel

  /** Given a virtual channel name, return the associated montage pair
    * for that channel
    */
  def getMontagePair(
    channelName: String,
    montageType: MontageType
  ): WithError[(String, Option[String])] =
    montageType match {
      case MontageType.NotMontaged => Right((channelName, None))
      case _ =>
        channelName.split(SEPARATOR).toList match {
          case head :: Nil => Right((head, None))
          case head :: last :: Nil => Right((head, Some(last)))
          case _ => Left(TimeSeriesException.InvalidMontageName(channelName))
        }
    }
}

/** Types of montages */
sealed trait MontageType {

  /** A list of channel pairs (leadChannel -> secondaryChannel) for
    * each montage.
    */
  val pairs: List[(String, String)]

  /** The set of virtual channel names for this montage type */
  def names: Set[String] =
    pairs.map(pair => Montage.getMontageName(pair._1, pair._2)).toSet

  /** The set of distinct channel names in the montage type */
  def distinctValues: Set[String] =
    pairs.flatMap(tup => List(tup._1, tup._2)).toSet
}
object MontageType {
  case object NotMontaged extends MontageType {
    override val pairs: List[(String, String)] = List.empty
  }

  case object BipolarAntPos extends MontageType {
    override val pairs = List(
      "Fp1" -> "F7",
      "F7" -> "T7",
      "T7" -> "P7",
      "P7" -> "O1",
      "Fp2" -> "F8",
      "F8" -> "T8",
      "T8" -> "P8",
      "P8" -> "O2",
      "Fp1" -> "F3",
      "F3" -> "C3",
      "C3" -> "P3",
      "P3" -> "O1",
      "Fp2" -> "F4",
      "F4" -> "C4",
      "C4" -> "P4",
      "P4" -> "O2",
      "Fz" -> "Cz",
      "Cz" -> "Fz"
    )
  }
  case object BipolarTransverse extends MontageType {
    override val pairs = List(
      "F7" -> "F3",
      "F3" -> "Fz",
      "Fz" -> "F4",
      "F4" -> "F8",
      "A1" -> "T7",
      "T7" -> "C3",
      "C3" -> "Cz",
      "Cz" -> "C4",
      "C4" -> "T8",
      "T8" -> "A2",
      "P7" -> "P3",
      "P3" -> "Pz",
      "Pz" -> "P4",
      "P4" -> "P8",
      "Fp1" -> "A1",
      "Fp2" -> "A2",
      "Q1" -> "A1",
      "Q2" -> "A2"
    )
  }
  case object ReferentialVsCz extends MontageType {
    override val pairs = List(
      "Fp1" -> "Cz",
      "Fp2" -> "Cz",
      "F7" -> "Cz",
      "F8" -> "Cz",
      "T7" -> "Cz",
      "T8" -> "Cz",
      "P7" -> "Cz",
      "P8" -> "Cz",
      "F3" -> "Cz",
      "F4" -> "Cz",
      "C3" -> "Cz",
      "C4" -> "Cz",
      "P3" -> "Cz",
      "P4" -> "Cz",
      "Q1" -> "Cz",
      "Q2" -> "Cz",
      "F2" -> "Cz",
      "P2" -> "Cz"
    )
  }
}
