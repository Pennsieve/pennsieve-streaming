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

import java.util.concurrent.ConcurrentHashMap
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.{ BinaryMessage, Message, TextMessage }
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.ThrottleMode.Shaping
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge, Partition }
import akka.stream.{ FlowShape, Graph, KillSwitches, OverflowStrategy, SharedKillSwitch }
import akka.util.ByteString
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.models.Channel
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query._
import com.pennsieve.streaming.server.StreamUtils.{ splitMerge, EitherOptionFilter }
import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.TimeSeriesFlow.{
  SessionFilters,
  SessionKillSwitches,
  SessionMontage,
  WithError
}
import com.pennsieve.streaming.{ RangeLookUp, TimeSeriesMessage, UnitRangeLookUp }
import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  InHandler,
  OutHandler,
  TimerGraphStageLogic
}
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.typesafe.config.Config
import scalikejdbc.DBSession
import spray.json._
import uk.me.berndporr.iirj.Butterworth

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

case class EpochedMessage[T](message: T, epoch: Long)

case class DualLaneBuffer(
  normalMessages: Vector[WithError[EpochedMessage[Respondable]]] = Vector.empty,
  shouldDump: Boolean = false,
  maxNormalSize: Int
) {

  def addMessage(
    message: WithError[EpochedMessage[Respondable]]
  )(implicit
    log: ContextLogger
  ): DualLaneBuffer = {
    // All messages go to normal lane now
    val updated = normalMessages :+ message
    if (updated.size > maxNormalSize) {
      val messagesToKeep = updated.takeRight(maxNormalSize)
//      log.noContext.debug(s"Buffer overflow: keeping latest ${messagesToKeep.size} messages")
      this.copy(normalMessages = messagesToKeep)
    } else {
      this.copy(normalMessages = updated)
    }
  }

  def setDumpRequested(): DualLaneBuffer = {
    this.copy(shouldDump = true)
  }

  def getAllMessagesForProcessing: Vector[WithError[EpochedMessage[Respondable]]] = {
    // Return all buffered messages for normal processing
    normalMessages
  }
}

/**
  * Created by jsnavely on 2/7/17.
  */
object TimeSeriesFlow extends Directives with TSJsonSupport {
  type SessionFilters =
    scala.collection.concurrent.Map[
      String,
      scala.collection.concurrent.Map[String, FilterStateTracker]
    ]
  type SessionMontage =
    scala.collection.concurrent.Map[String, scala.collection.concurrent.Map[String, MontageType]]

  type SessionKillSwitches =
    scala.collection.concurrent.Map[String, scala.collection.concurrent.Map[Long, SharedKillSwitch]]

  type WithError[B] = Either[TimeSeriesException, B]
  type WithErrorT[B] = EitherT[Future, TimeSeriesException, B]

  def routeFlow(
    flow: EitherT[Future, TimeSeriesException, Flow[Message, Message, NotUsed]]
  ): Route =
    onComplete(flow.value) {
      case Success(either) =>
        either.fold(e => {
          e.printStackTrace()
          complete { e.statusCode -> e }
        }, handleWebSocketMessages)
      case Failure(unexpected) => {
        unexpected.printStackTrace()
        val error = TimeSeriesException.UnexpectedError(unexpected.toString)
        complete {
          error.statusCode -> error
        }
      }
    }
}

class TimeSeriesFlow(
  session: String,
  sessionFilters: SessionFilters,
  sessionMontage: SessionMontage,
  sessionKillSwitches: SessionKillSwitches,
  channelMap: Map[String, Channel],
  rangeLookup: RangeLookUp,
  unitRangeLookUp: UnitRangeLookUp,
  startAtEpoch: Boolean = false
)(implicit
  log: ContextLogger,
  dbSession: DBSession,
  ec: ExecutionContext,
  system: ActorSystem,
  config: Config,
  wsClient: WsClient
) extends TSJsonSupport {

  val flowId: Long = System.currentTimeMillis()
  val continuousQueryExecutor = new TimeSeriesQueryRawHttp(wsClient)
  val unitQueryExecutor = new TimeSeriesUnitQueryRawHttp(config, wsClient)
  val parallelism = config.getInt("timeseries.parallelism")
  val throttleItems = config.getInt("timeseries.throttle.items")
  val throttlePeriod = config.getInt("timeseries.throttle.period")
  val inactiveTimeout = config.getDuration("timeseries.idle-timeout")
  val maxMessageQueue = config.getInt("timeseries.max-message-queue")
  var lastActive = System.currentTimeMillis()

  // Epoch-based message invalidation (cleaner than timestamps)
  @volatile var currentEpoch: Long = -1L

  // Session state management
  val channelFilters =
    sessionFilters.getOrElse(session, new ConcurrentHashMap[String, FilterStateTracker]().asScala)
  var packageMontages =
    sessionMontage.getOrElse(session, new ConcurrentHashMap[String, MontageType]().asScala)
  var killSwitches =
    sessionKillSwitches.getOrElse(session, new ConcurrentHashMap[Long, SharedKillSwitch]().asScala)
  val packageMinimumTime = channelMap.values.map(_.start).min
  val killswitch = KillSwitches.shared(session)
  killSwitches.put(flowId, killswitch)

  @volatile var pendingBufferDump: Boolean = false

  def shouldDiscardMessage(messageEpoch: Long): Boolean = {
    messageEpoch < currentEpoch
  }

  // Add message filtering flow for epoched messages
  def createMessageFilter[
    T
  ]: Flow[WithError[EpochedMessage[T]], WithError[EpochedMessage[T]], NotUsed] = {
    Flow[WithError[EpochedMessage[T]]]
      .filter {
        case Right(epochedMsg) =>
          val discard = shouldDiscardMessage(epochedMsg.epoch)
          if (discard) {
            log.noContext.debug(
              s"Discarding message with epoch ${epochedMsg.epoch}, current epoch: $currentEpoch"
            )
          }
          !discard
        case Left(_) => true // Always pass through errors
      }
  }

  // Modified flows to handle epoched messages and filtering
  val unitDataMultiFlow: Flow[EpochedMessage[TimeSeriesRequest], WithError[
    EpochedMessage[UnitRangeRequest]
  ], NotUsed] =
    Flow[EpochedMessage[TimeSeriesRequest]]
      .filter(msg => !shouldDiscardMessage(msg.epoch))
      .map { epochedTsr =>
        val tsr = epochedTsr.message
        // Extract channel IDs from either channelIds or virtualChannels
        val channelIds = tsr.channelIds.getOrElse {
          tsr.virtualChannels.getOrElse(List.empty).map(_.id)
        }

        channelIds match {
          case Nil => Left(TimeSeriesException.UnexpectedError("No channels specified in request"))
          case channelIds =>
            val missingChannels = channelIds.toSet -- channelMap.keySet
            if (missingChannels.isEmpty)
              Right(
                channelIds
                  .map(channelMap)
                  .flatMap(tsr.toUnitRangeRequests(unitRangeLookUp))
                  .map(req => EpochedMessage(req, epochedTsr.epoch))
              )
            else
              Left(
                TimeSeriesException
                  .PackageMissingChannels(missingChannels.toList, tsr.packageId)
              )
        }
      }
      // flatten this stream of lists
      .mapConcat(_.fold(e => List(Left(e)), requests => requests.map(Right(_))))

  val timeSeriesMultiFlow: Flow[EpochedMessage[TimeSeriesRequest], WithError[
    EpochedMessage[(RangeRequest, Option[RangeRequest])]
  ], NotUsed] =
    Flow[EpochedMessage[TimeSeriesRequest]]
      .filter(msg => !shouldDiscardMessage(msg.epoch))
      .map { epochedTsr =>
        val tsr = epochedTsr.message
        for {

          virtualChannels <- tsr.getVirtualChannels(channelMap)

          _ = {
            log.noContext.debug(s"${channelMap}")
            val currentMontage = packageMontages.get(tsr.packageId)
            log.noContext.debug(
              s"Before buildMontage: packageId=${tsr.packageId}, currentMontage=$currentMontage"
            )
            log.noContext.debug(s"Available channels: ${channelMap.keys.mkString(", ")}")
            log.noContext.debug(s"Virtual channels: ${virtualChannels.map(_.id).mkString(", ")}")
          }

          // build the montage pairs for this set of channels based
          // on the current montage
          montagedChannels <- Montage.buildMontage(
            packageMontages,
            tsr.packageId,
            channelMap,
            virtualChannels
          )

          // align the lead and secondary channel requests in order
          // to ensure we are montaging the correct data between the
          // channels
          alignedRequests <- montagedChannels.flatMap {
            case (leadChannel, maybeSecondaryChannel) => {
              val leadChannelRequests =
                tsr.toContinuousRangeRequests(rangeLookup)(leadChannel)

              maybeSecondaryChannel
                .map(tsr.toContinuousRangeRequests(rangeLookup)) match {
                case Some(secondaryChannelRequests) => {

                  // If the amount of requests differs between the lead/secondary
                  // channel for this montage, the data is misaligned
                  if (leadChannelRequests.length != secondaryChannelRequests.length)
                    List(
                      Left(
                        TimeSeriesException
                          .MontageMisalignment(leadChannel.nodeId, maybeSecondaryChannel.get.nodeId)
                      )
                    )

                  // Otherwise zip the requests together to align them
                  else
                    leadChannelRequests
                      .sortBy(_.sequenceId)
                      .zip(secondaryChannelRequests.sortBy(_.sequenceId))
                      .map {
                        case (l, s) =>
                          Right(EpochedMessage((l, Some(s)), epochedTsr.epoch)): WithError[
                            EpochedMessage[(RangeRequest, Option[RangeRequest])]
                          ]
                      }
                }
                case None =>
                  leadChannelRequests.map(
                    r =>
                      Right(EpochedMessage((r, None), epochedTsr.epoch)): WithError[
                        EpochedMessage[(RangeRequest, Option[RangeRequest])]
                      ]
                  )
              }
            }
          }.sequence
        } yield alignedRequests
      }
      // flatten out this stream of lists
      .mapConcat(_.fold(e => List(Left(e)), requests => requests.map(Right(_))))

  val queryUnitHttpS3ExecFlow: Flow[WithError[EpochedMessage[UnitRangeRequest]], WithError[
    EpochedMessage[TimeSeriesMessage]
  ], NotUsed] =
    Flow[WithError[EpochedMessage[UnitRangeRequest]]]
      .via(createMessageFilter[UnitRangeRequest])
      .filter(
        _.fold(
          _ => true,
          epochedRr => channelTypeMatch(epochedRr.message.channel, "unit", channelMap)
        )
      )
      .mapAsyncUnordered(parallelism) {
        _.fold(
          e => Future.successful(Left(e)),
          epochedRr => {
            if (shouldDiscardMessage(epochedRr.epoch)) {
              Future.successful(
                Left(TimeSeriesException.UnexpectedError("Message discarded due to buffer dump"))
              )
            } else {
              unitQueryExecutor
                .rangeQuery(epochedRr.message)
                .map(result => Right(EpochedMessage(result, epochedRr.epoch)))
            }
          }
        )
      }

  val queryHttpS3ExecFlow
    : Flow[WithError[EpochedMessage[(RangeRequest, Option[RangeRequest])]], WithError[
      EpochedMessage[TimeSeriesMessage]
    ], NotUsed] =
    Flow[WithError[EpochedMessage[(RangeRequest, Option[RangeRequest])]]]
      .via(createMessageFilter[(RangeRequest, Option[RangeRequest])])
      .filter(
        _.fold(
          _ => true,
          epochedChannels => {
            val channels = epochedChannels.message
            (channels._1.channelNodeId :: channels._2
              .map(_.channelNodeId)
              .toList)
              .forall(channel => channelTypeMatch(channel, "continuous", channelMap))
          }
        )
      )
      .mapAsyncUnordered(parallelism) {
        _.fold(
          e => Future.successful(Left(e)),
          epochedChannels => {
            if (shouldDiscardMessage(epochedChannels.epoch)) {
              Future.successful(
                Left(TimeSeriesException.UnexpectedError("Message discarded due to buffer dump"))
              )
            } else {
              val (leadChannel, secondaryChannel) = epochedChannels.message
              continuousQueryExecutor
                .rangeQuery(leadChannel, channelFilters, secondaryChannel)
                .map(result => Right(EpochedMessage(result, epochedChannels.epoch)))
            }
          }
        )
      }

  val dualQueryExecutor: Graph[FlowShape[EpochedMessage[TimeSeriesRequest], WithError[
    EpochedMessage[TimeSeriesMessage]
  ]], NotUsed] =
    splitMerge(
      unitDataMultiFlow.via(queryUnitHttpS3ExecFlow),
      timeSeriesMultiFlow.via(queryHttpS3ExecFlow)
    )

  val resetResponseTimestamps: Flow[WithError[EpochedMessage[TimeSeriesMessage]], WithError[
    EpochedMessage[TimeSeriesMessage]
  ], NotUsed] =
    Flow[WithError[EpochedMessage[TimeSeriesMessage]]]
      .via(createMessageFilter[TimeSeriesMessage])
      .map(_.map { epochedMsg =>
        val msg = epochedMsg.message
        val updatedMsg = msg match {
          case TimeSeriesMessage(
              segment,
              event,
              instruction,
              ingestSegment,
              totalResponses,
              responseSequenceId
              ) if startAtEpoch =>
            TimeSeriesMessage(
              segment.map(
                s =>
                  s.copy(
                    pageStart = s.pageStart - packageMinimumTime,
                    pageEnd = s.pageEnd - packageMinimumTime,
                    startTs = s.startTs - packageMinimumTime
                  )
              ),
              event.map(
                e =>
                  e.copy(
                    pageStart = e.pageStart - packageMinimumTime,
                    pageEnd = e.pageEnd - packageMinimumTime,
                    times = e.times
                      .grouped(2)
                      .flatMap {
                        case Seq(time, count) =>
                          Seq(time - packageMinimumTime, count)
                      }
                      .toSeq
                  )
              ),
              instruction,
              ingestSegment,
              totalResponses,
              responseSequenceId
            )
          case passthrough => passthrough
        }
        EpochedMessage(updatedMsg, epochedMsg.epoch)
      })

  val toWsMessage: Flow[WithError[EpochedMessage[TimeSeriesMessage]], Message, NotUsed] =
    Flow[WithError[EpochedMessage[TimeSeriesMessage]]]
      .via(createMessageFilter[TimeSeriesMessage])
      .map(
        _.fold(
          e => {
            log.noContext.debug(s"toWsMessage: Converting error to TextMessage: $e")
            TextMessage(e.json)
          },
          epochedMsg => {
            log.noContext.debug(
              s"toWsMessage: Converting TimeSeriesMessage to BinaryMessage, epoch=${epochedMsg.epoch}"
            )
            BinaryMessage(ByteString(epochedMsg.message.toByteArray))
          }
        )
      )

  // Modified montage channels flow to handle epoched messages
  val listMontageChannels: Flow[EpochedMessage[MontageRequest], Message, NotUsed] =
    Flow[EpochedMessage[MontageRequest]]
      .filter(msg => !shouldDiscardMessage(msg.epoch))
      .map { epochedMr =>
        val mr = epochedMr.message
        mr match {
          case mr @ MontageRequest(_, MontageType.NotMontaged, _) => {
            // set the montage state
            buildMontage(mr)

            val packageVirtualChannels = channelMap.map {
              case (id, channel) =>
                VirtualChannelInfo(
                  id = id,
                  name = channel.name,
                  start = channel.start,
                  end = channel.end,
                  channelType = channel.`type`,
                  rate = channel.rate,
                  unit = channel.unit
                )
            }.toList

            Right(ChannelsDetailsList(channelDetails = packageVirtualChannels))
          }
          case mr @ MontageRequest(_, MontageType.CustomMontage(), montageMap) => {
            val customMontage = MontageType.CustomMontage()
            customMontage.updatePairs(montageMap.get)
            customMontage.pairs
              .map {
                case (leadChannelName, secondaryChannelName) =>
                  // return an error if the package is not montageable
                  Montage
                    .checkMontageability(MontageType.CustomMontage(), channelMap)
                    .map { _ =>
                      // This package is montageable with this montage, so we can
                      // set the montage state.
                      // (If the package is not montageable, the montage state will
                      // not be set to the montage in this request).
                      buildMontage(mr)

                      val leadChannel =
                        channelMap.values
                          .find(c => c.name == leadChannelName)
                          .get

                      VirtualChannelInfo(
                        id = leadChannel.nodeId,
                        name = Montage.getMontageName(leadChannelName, Some(secondaryChannelName)),
                        start = leadChannel.start,
                        end = leadChannel.end,
                        channelType = leadChannel.`type`,
                        rate = leadChannel.rate,
                        unit = leadChannel.unit
                      )
                    }
              }
              .sequence
              .map(virtualChannels => ChannelsDetailsList(channelDetails = virtualChannels))
          }
          case mr @ MontageRequest(_, montageType, _) =>
            montageType.pairs
              .map {
                case (leadChannelName, secondaryChannelName) =>
                  // return an error if the package is not montageable
                  Montage
                    .checkMontageability(montageType, channelMap)
                    .map { _ =>
                      // This package is montageable with this montage, so we can
                      // set the montage state.
                      // (If the package is not montageable, the montage state will
                      // not be set to the montage in this request).
                      buildMontage(mr)

                      val leadChannel =
                        channelMap.values
                          .find(c => c.name == leadChannelName)
                          .get

                      VirtualChannelInfo(
                        id = leadChannel.nodeId,
                        name = Montage.getMontageName(leadChannelName, Some(secondaryChannelName)),
                        start = leadChannel.start,
                        end = leadChannel.end,
                        channelType = leadChannel.`type`,
                        rate = leadChannel.rate,
                        unit = leadChannel.unit
                      )
                    }
              }
              .sequence
              .map(virtualChannels => ChannelsDetailsList(channelDetails = virtualChannels))
        }
      }
      .map(_.fold(e => TextMessage(e.json), is => TextMessage(is.toJson.toString)))

  def parseFlow: Flow[Message, WithError[EpochedMessage[Respondable]], NotUsed] = {
    Flow[Message]
      .throttle(throttleItems, throttlePeriod.second, throttleItems, Shaping)
      .via(killswitch.flow)
      .keepAlive(15.seconds, () => TextMessage.Strict(new KeepAlive().toJson.toString))
      .mapAsync(1) {
        case textMessage: TextMessage =>
          textMessage.toStrict(10 seconds).map { message =>
            val messageEpoch = currentEpoch // Capture current epoch for this message

            val timeSeriesRequest = Try(message.text.parseJson.convertTo[TimeSeriesRequest])
            val montageRequest = Try(message.text.parseJson.convertTo[MontageRequest])
            val dumpBufferRequest = Try(message.text.parseJson.convertTo[DumpBufferRequest])

            if (dumpBufferRequest.isSuccess) {
              lastActive = System.currentTimeMillis()
              val dumpReq = dumpBufferRequest.get

              // INSTANTANEOUSLY increment epoch - all existing messages become invalid
              currentEpoch += 1
              log.noContext.debug(s"Buffer dump requested: starting new epoch $currentEpoch")

              // Return dump request with NEW epoch so it doesn't get filtered out
              Right(Some(EpochedMessage(dumpReq: Respondable, currentEpoch)))

            } else if (timeSeriesRequest.isSuccess) {
              lastActive = System.currentTimeMillis()
              val epochedMsg = EpochedMessage(timeSeriesRequest.get: Respondable, messageEpoch)
              log.noContext.debug(
                s"Created epoched message: epoch=$messageEpoch, currentEpoch=$currentEpoch, ${epochedMsg.message}, type=${epochedMsg.message.getClass.getSimpleName}"
              )
              Right(Some(epochedMsg))
//              Right(Some(EpochedMessage(timeSeriesRequest.get: Respondable, messageEpoch)))
            }
            // Then MontageRequest
            else if (montageRequest.isSuccess) {
              lastActive = System.currentTimeMillis()
              Right(Some(EpochedMessage(montageRequest.get: Respondable, messageEpoch)))
            } else {
              // if we didn't get respondable request, attempt to parse all other options
              perform(message.text) match {
                case Success(_) => Right(None)
                case Failure(_) =>
                  Left(
                    TimeSeriesException
                      .UnexpectedError(s"unsupported message received: $message")
                  )
              }
            }
          }
        case _: BinaryMessage =>
          Future.successful(
            Left(
              TimeSeriesException
                .UnexpectedError("received unexpected binary message")
            )
          )
      }
      .via(EitherOptionFilter)
      .via(new BufferWithEpochDumpStage(maxMessageQueue, 50.milliseconds))
  }

  val resetRequestTimestamps
    : Flow[EpochedMessage[TimeSeriesRequest], EpochedMessage[TimeSeriesRequest], NotUsed] =
    Flow[EpochedMessage[TimeSeriesRequest]]
      .filter(msg => {
        val shouldDiscard = shouldDiscardMessage(msg.epoch)
        log.noContext.debug(
          s"resetRequestTimestamps: epoch=${msg.epoch}, currentEpoch=$currentEpoch, shouldDiscard=$shouldDiscard"
        )
        !shouldDiscard
      })
      .map { epochedTsr =>
        log.noContext.debug(
          s"resetRequestTimestamps: Processing message with epoch ${epochedTsr.epoch}"
        )
        val tsr = epochedTsr.message
        val updatedTsr = tsr match {
          case tsr if startAtEpoch =>
            tsr.copy(
              startTime = packageMinimumTime + tsr.startTime,
              endTime = packageMinimumTime + tsr.endTime
            )
          case passthrough => passthrough
        }
        EpochedMessage(updatedTsr, epochedTsr.epoch)
      }

  // Create a simple partition flow that mimics the original RespondablePartition
  def createEpochedPartition: Flow[WithError[EpochedMessage[Respondable]], Either[
    TimeSeriesException,
    Either[EpochedMessage[MontageRequest], EpochedMessage[TimeSeriesRequest]]
  ], NotUsed] = {
    Flow[WithError[EpochedMessage[Respondable]]]
      .map {
        case Left(error) =>
          log.noContext.debug(s"createEpochedPartition: Error - $error")
          Left(error)
        case Right(epochedMsg) =>
          epochedMsg.message match {
            case _: MontageRequest =>
              log.noContext.debug(
                s"createEpochedPartition: MontageRequest with epoch ${epochedMsg.epoch}"
              )
              Right(Left(epochedMsg.asInstanceOf[EpochedMessage[MontageRequest]]))
            case _: TimeSeriesRequest =>
              log.noContext.debug(
                s"createEpochedPartition: TimeSeriesRequest with epoch ${epochedMsg.epoch}"
              )
              Right(Right(epochedMsg.asInstanceOf[EpochedMessage[TimeSeriesRequest]]))
            case _ =>
              log.noContext.debug(
                s"createEpochedPartition: Unknown type ${epochedMsg.message.getClass.getSimpleName}"
              )
              Left(TimeSeriesException.UnexpectedError("Unknown respondable type"))
          }
      }
  }

  val flowGraph: Graph[FlowShape[Message, Message], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // attempt to parse all incoming binary websocket messages,
      // modifying server state where necessary, and filtering out
      // messages that won't require a response
      val parser = builder.add(parseFlow)

      // Partition messages into different types
      val partitioner = builder.add(createEpochedPartition)

      // Split the partitioned stream
      val partition = builder.add(
        Partition[Either[TimeSeriesException, Either[EpochedMessage[MontageRequest], EpochedMessage[
          TimeSeriesRequest
        ]]]](3, {
          case Left(_) => 0 // Errors
          case Right(Left(_)) => 1 // Montage requests
          case Right(Right(_)) => 2 // TimeSeries requests
        })
      )

      // Extract specific types from each partition
      val errorExtractor = builder.add(
        Flow[Either[TimeSeriesException, Either[EpochedMessage[MontageRequest], EpochedMessage[
          TimeSeriesRequest
        ]]]]
          .mapConcat {
            case Left(error) => List(error)
            case _ => List.empty
          }
      )

      val montageExtractor = builder.add(
        Flow[Either[TimeSeriesException, Either[EpochedMessage[MontageRequest], EpochedMessage[
          TimeSeriesRequest
        ]]]]
          .mapConcat {
            case Right(Left(montageMsg)) => List(montageMsg)
            case _ => List.empty
          }
      )

      val timeSeriesExtractor = builder.add(
        Flow[Either[TimeSeriesException, Either[EpochedMessage[MontageRequest], EpochedMessage[
          TimeSeriesRequest
        ]]]]
          .mapConcat {
            case Right(Right(tsMsg)) =>
              log.noContext.debug(
                s"timeSeriesExtractor: Extracted TimeSeriesRequest with epoch ${tsMsg.epoch}"
              )
              List(tsMsg)
            case Right(Left(montageMsg)) =>
              log.noContext.debug(s"timeSeriesExtractor: Got MontageRequest (should be empty)")
              List.empty
            case Left(error) =>
              log.noContext.debug(s"timeSeriesExtractor: Got error (should be empty)")
              List.empty
          }
      )

      // reset all timestamps in the incoming TimeSeriesRequest
      val requestTimestampResetter = builder.add(resetRequestTimestamps)

      // query for timeseries data and respond with a datastream given
      // a timeseries request
      val queryer = builder.add(dualQueryExecutor)

      // reset all timestamps in the outgoing TimeSeriesMessage
      val responseTimestampResetter = builder.add(resetResponseTimestamps)

      // convert outgoing protobuf messages to akka messages that can
      // be sent across the wire
      val wsMessageConverter = builder.add(toWsMessage)

      // return a list of virtual channels associated with the given
      // montage. this processor will return an error if the package
      // is not eligible for the given montage.
      val montageLister = builder.add(listMontageChannels)

      // Convert parse errors to text messages
      val errorConverter = builder.add(Flow[TimeSeriesException].map(e => TextMessage(e.json)))

      // merge all responses back into a single outgoing flow
      val merge = builder.add(Merge[Message](3))

      // format: off
      parser.out ~> partitioner ~> partition

      partition.out(0) ~> errorExtractor ~> errorConverter ~> merge
      partition.out(1) ~> montageExtractor ~> montageLister ~> merge
      partition.out(2) ~> timeSeriesExtractor ~> requestTimestampResetter ~> queryer ~> responseTimestampResetter ~> wsMessageConverter ~> merge
      // format: on

      FlowShape(parser.in, merge.out)

    }

  // Updated BufferWithEpochDumpStage to handle epoched messages
  class BufferWithEpochDumpStage(
    maxSize: Int,
    flushTimeout: FiniteDuration
  )(implicit
    log: ContextLogger
  ) extends GraphStage[
        FlowShape[WithError[EpochedMessage[Respondable]], WithError[EpochedMessage[Respondable]]]
      ] {

    val in = Inlet[WithError[EpochedMessage[Respondable]]]("BufferDump.in")
    val out = Outlet[WithError[EpochedMessage[Respondable]]]("BufferDump.out")
    override val shape = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) {

        private var pendingBuffer = Vector.empty[WithError[EpochedMessage[Respondable]]]
        private val FlushTimer = "flush"

        override def preStart(): Unit = {
          scheduleOnce(FlushTimer, flushTimeout)
        }

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              grab(in) match {
                case Right(epochedMsg) if epochedMsg.message.isInstanceOf[DumpBufferRequest] =>
                  log.noContext.debug(
                    s"Global dump request - clearing ${pendingBuffer.size} pending messages"
                  )
                  pendingBuffer = Vector.empty // GLOBAL DUMP - clears ALL pending messages
                  safePullIfNeeded() // Consume the dump request and continue

                case message =>
                  // Filter out messages that should be discarded due to old epoch
                  message match {
                    case Right(epochedMsg) if shouldDiscardMessage(epochedMsg.epoch) =>
                      log.noContext.debug(
                        s"Discarding message in buffer stage with epoch ${epochedMsg.epoch}, current epoch: $currentEpoch"
                      )
                      safePullIfNeeded()
                    case _ =>
                      pendingBuffer = pendingBuffer :+ message

                      // Flush if buffer is full
                      if (pendingBuffer.size >= maxSize) {
                        flushBuffer()
                      } else {
                        pull(in)
                      }
                  }
              }
            }

            override def onUpstreamFinish(): Unit = {
              log.noContext.debug(
                s"Upstream finished - flushing ${pendingBuffer.size} remaining messages"
              )
              // Cancel timer to prevent future attempts to pull
              cancelTimer(FlushTimer)

              // Emit any remaining messages
              if (pendingBuffer.nonEmpty) {
                emitMultiple(out, pendingBuffer)
                pendingBuffer = Vector.empty
              }

              // Complete the stage
              completeStage()
            }

            override def onUpstreamFailure(ex: Throwable): Unit = {
              cancelTimer(FlushTimer)
              failStage(ex)
            }

          }
        )

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (!hasBeenPulled(in)) pull(in)
          }
        })

        override protected def onTimer(timerKey: Any): Unit = {
          if (timerKey == FlushTimer) {
            // Only flush if upstream is still active
            if (!isClosed(in)) {
              flushBuffer()
              // Reschedule only if upstream is still active
              scheduleOnce(FlushTimer, flushTimeout)
            }
          }
        }

        private def flushBuffer(): Unit = {
          if (pendingBuffer.nonEmpty) {
            log.noContext.debug(s"Timer flush: sending ${pendingBuffer.size} messages downstream")
            emitMultiple(out, pendingBuffer)
            pendingBuffer = Vector.empty
          }
          safePullIfNeeded()
        }

        private def safePullIfNeeded(): Unit = {
          if (!hasBeenPulled(in) && !isClosed(in) && isAvailable(out)) {
            pull(in)
          }
        }
      }
  }

  // Rest of your existing methods remain the same...
  def numberSequentially[T](ls: Seq[T])(numberit: ((T, Int)) => T): Seq[T] =
    ls.zipWithIndex.map(numberit)

  def channelTypeMatch(channelId: String, ctype: String, cmap: Map[String, Channel]): Boolean = {
    val matches = for {
      c <- cmap.get(channelId)
    } yield c.`type`.toLowerCase == ctype.toLowerCase
    matches getOrElse false
  }

  def perform(msg: String): Try[Boolean] = {
    Try(msg.parseJson.convertTo[FilterRequest])
      .map(buildFilters _) orElse
      Try(msg.parseJson.convertTo[ClearFilterRequest])
        .map(clearFilters _) orElse
      Try(msg.parseJson.convertTo[ResetFilterRequest])
        .map(resetFilters _) orElse
      Try(msg.parseJson.convertTo[KeepAlive]).map(killInactive _)
  }

  def dumpBuffer(req: DumpBufferRequest): Boolean = {
    log.noContext.debug(s"Buffer dump requested: $req")
    currentEpoch += 1
    log.noContext.debug(s"Started new epoch: $currentEpoch")
    true
  }

  def buildFilters(req: FilterRequest): Boolean = {
    req.channels foreach { channelId =>
      channelMap
        .get(channelId)
        .foreach(channel => {
          channelFilters.put(channelId, buildFilter(req, channel.rate))
          sessionFilters.put(session, channelFilters)
        })
    }
    true
  }

  def buildFilter(filterRequest: FilterRequest, rate: Double): FilterStateTracker = {
    val filterorder = filterRequest.filterParameters.head.toInt
    val filterFreq = filterRequest.filterParameters(1)
    val butterworth = new Butterworth()

    var maxFilterFreq = filterFreq

    filterRequest.filter.toLowerCase match {

      case "bandstop" =>
        val filterWidth = filterRequest.filterParameters(2)
        butterworth.bandStop(filterorder, rate, filterFreq, filterWidth)
        maxFilterFreq = maxFilterFreq + filterWidth

      case "bandpass" =>
        val filterWidth = filterRequest.filterParameters(2)
        butterworth.bandPass(filterorder, rate, filterFreq, filterWidth)
        maxFilterFreq = maxFilterFreq + filterWidth

      case "highpass" => butterworth.highPass(filterorder, rate, filterFreq)

      case "lowpass" => butterworth.lowPass(filterorder, rate, filterFreq)

      case unknown =>
        log.noContext.error("Received unrecognized filter type:" + unknown)
    }
    new FilterStateTracker(butterworth, filterorder, maxFilterFreq)
  }

  def buildMontage(req: MontageRequest): MontageRequest = {
    req match {
      case MontageRequest(packageId, MontageType.NotMontaged, _) => {
        packageMontages.remove(packageId)
      }
      case MontageRequest(packageId, MontageType.CustomMontage(), montageMap) => {
        log.noContext.info("Getting custom Montage " + montageMap.get)
        val customMontage = MontageType.CustomMontage()
        customMontage.updatePairs(montageMap.get) // Set the custom pairs
        packageMontages.put(packageId, customMontage)
      }
      case MontageRequest(packageId, mt, _) => {
        packageMontages.put(packageId, mt)
      }
    }

    sessionMontage.put(session, packageMontages)
    req
  }

  def clearFilters(req: ClearFilterRequest): Boolean = {
    req.channelFiltersToClear foreach { c =>
      channelFilters.remove(c)
    }
    sessionFilters.put(session, channelFilters)
    true
  }

  def resetFilters(req: ResetFilterRequest): Boolean = {
    req.channelFiltersToReset foreach { channel =>
      {
        channelFilters.get(channel).foreach(filter => filter.reset())
      }
    }
    true
  }

  def killInactive(req: KeepAlive): Boolean = {
    val freshness = req.currentTime - lastActive
    if (freshness > inactiveTimeout.toMillis) {
      log.noContext.warn(s"killing connection session $session age $freshness")
      killswitch.shutdown()
      true
    } else {
      false
    }
  }

}
