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
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge }
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

case class DualLaneBuffer(
  normalMessages: Vector[WithError[Respondable]] = Vector.empty,
  shouldDump: Boolean = false,
  maxNormalSize: Int
) {

  def addMessage(message: WithError[Respondable])(implicit log: ContextLogger): DualLaneBuffer = {
    // All messages go to normal lane now
    val updated = normalMessages :+ message
    if (updated.size > maxNormalSize) {
      val messagesToKeep = updated.takeRight(maxNormalSize)
      log.noContext.debug(s"Buffer overflow: keeping latest ${messagesToKeep.size} messages")
      this.copy(normalMessages = messagesToKeep)
    } else {
      this.copy(normalMessages = updated)
    }
  }

  def setDumpRequested(): DualLaneBuffer = {
    this.copy(shouldDump = true)
  }

  def getAllMessagesForProcessing: Vector[WithError[Respondable]] = {
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

//  type flowId = Long

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

  val unitDataMultiFlow: Flow[TimeSeriesRequest, WithError[UnitRangeRequest], NotUsed] =
    Flow[TimeSeriesRequest]
      .map(
        tsr =>
          tsr.channelIds.flatMap { channelIds =>
            val missingChannels = channelIds.toSet -- channelMap.keySet
            if (missingChannels.isEmpty)
              Right(
                channelIds
                  .map(channelMap)
                  .flatMap(tsr.toUnitRangeRequests(unitRangeLookUp))
              )
            else
              Left(
                TimeSeriesException
                  .PackageMissingChannels(missingChannels.toList, tsr.packageId)
              )
          }
      )

      // flatten this stream of lists
      .mapConcat(_.fold(e => List(Left(e)), requests => requests.map(Right(_))))

  val timeSeriesMultiFlow
    : Flow[TimeSeriesRequest, WithError[(RangeRequest, Option[RangeRequest])], NotUsed] =
    Flow[TimeSeriesRequest]
      .map { tsr =>
        for {
          virtualChannels <- tsr.getVirtualChannels(channelMap)

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
                          Right(l -> (Some(s)))
                      }
                }
                case None =>
                  leadChannelRequests.map(r => Right(r -> None))
              }
            }
          }.sequence
        } yield alignedRequests
      }

      // flatten out this stream of lists
      .mapConcat(_.fold(e => List(Left(e)), requests => requests.map(Right(_))))

  val queryUnitHttpS3ExecFlow
    : Flow[WithError[UnitRangeRequest], WithError[TimeSeriesMessage], NotUsed] =
    Flow[WithError[UnitRangeRequest]]
      .filter(_.fold(_ => true, rr => channelTypeMatch(rr.channel, "unit", channelMap)))
      .mapAsyncUnordered(parallelism) {
        _.fold(
          e => Future.successful(Left(e)),
          rr => unitQueryExecutor.rangeQuery(rr).map(Right.apply)
        )
      }

  val queryHttpS3ExecFlow
    : Flow[WithError[(RangeRequest, Option[RangeRequest])], WithError[TimeSeriesMessage], NotUsed] =
    Flow[WithError[(RangeRequest, Option[RangeRequest])]]
      .filter(
        _.fold(
          _ => true,
          channels => {
            (channels._1.channelNodeId :: channels._2
              .map(_.channelNodeId)
              .toList)
              .forall(channel => channelTypeMatch(channel, "continuous", channelMap))
          }
        )
      )
      .mapAsyncUnordered(parallelism) {
        _.fold(
          e => Future.successful(Left(e)), {
            case (leadChannel, secondaryChannel) =>
              continuousQueryExecutor
                .rangeQuery(leadChannel, channelFilters, secondaryChannel)
                .map(Right.apply)
          }
        )
      }

  val dualQueryExecutor
    : Graph[FlowShape[TimeSeriesRequest, WithError[TimeSeriesMessage]], NotUsed] =
    splitMerge(
      unitDataMultiFlow
        .via(queryUnitHttpS3ExecFlow),
      timeSeriesMultiFlow
        .via(queryHttpS3ExecFlow)
    )

  val resetResponseTimestamps
    : Flow[WithError[TimeSeriesMessage], WithError[TimeSeriesMessage], NotUsed] =
    Flow[WithError[TimeSeriesMessage]]
      .map(_.map {
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
      })

  val toWsMessage: Flow[WithError[TimeSeriesMessage], Message, NotUsed] =
    Flow[WithError[TimeSeriesMessage]]
      .map(_.fold(e => TextMessage(e.json), is => BinaryMessage(ByteString(is.toByteArray))))

  // Respond to `MontageRequest` objects with a list of
  // VirtualChannels for the given montage
  val listMontageChannels: Flow[MontageRequest, Message, NotUsed] =
    Flow[MontageRequest]
      .map {
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
      .map(_.fold(e => TextMessage(e.json), is => TextMessage(is.toJson.toString)))

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
    log.noContext.info(s"Buffer dump requested: $req")
    pendingBufferDump = true
    true // Return success like other handlers
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

  class BufferWithGlobalDumpStage(
    maxSize: Int,
    flushTimeout: FiniteDuration
  )(implicit
    log: ContextLogger
  ) extends GraphStage[FlowShape[WithError[Respondable], WithError[Respondable]]] {

    val in = Inlet[WithError[Respondable]]("BufferDump.in")
    val out = Outlet[WithError[Respondable]]("BufferDump.out")
    override val shape = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) {

        private var pendingBuffer = Vector.empty[WithError[Respondable]]
        private val FlushTimer = "flush"

        override def preStart(): Unit = {
          scheduleOnce(FlushTimer, flushTimeout)
        }

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              grab(in) match {
                case Right(_: DumpBufferRequest) =>
                  log.noContext.info(
                    s"Global dump request - discarding ${pendingBuffer.size} pending messages"
                  )
                  pendingBuffer = Vector.empty // GLOBAL DUMP - clears ALL pending messages
                  safePullIfNeeded() // Consume the dump request and continue

                case message =>
                  pendingBuffer = pendingBuffer :+ message

                  // Flush if buffer is full
                  if (pendingBuffer.size >= maxSize) {
                    flushBuffer()
                  } else {
                    pull(in)
                  }
              }
            }

            override def onUpstreamFinish(): Unit = {
              log.noContext.info(
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

  def parseFlow: Flow[Message, WithError[Respondable], NotUsed] = {
    Flow[Message]
      .throttle(throttleItems, throttlePeriod.second, throttleItems, Shaping)
      .via(killswitch.flow)
      .keepAlive(15.seconds, () => TextMessage.Strict(new KeepAlive().toJson.toString))
      .mapAsync(1) {
        case textMessage: TextMessage =>
          textMessage.toStrict(10 seconds).map { message =>
            val timeSeriesRequest = Try(message.text.parseJson.convertTo[TimeSeriesRequest])
            val montageRequest = Try(message.text.parseJson.convertTo[MontageRequest])
            val dumpBufferRequest = Try(message.text.parseJson.convertTo[DumpBufferRequest])

            if (timeSeriesRequest.isSuccess) {
              lastActive = System.currentTimeMillis()
              Right(Some(timeSeriesRequest.get: Respondable))
            }
            // Then MontageRequest
            else if (montageRequest.isSuccess) {
              lastActive = System.currentTimeMillis()
              Right(Some(montageRequest.get: Respondable))
            }
            // Then DumpBufferRequest
            else if (dumpBufferRequest.isSuccess) {
              lastActive = System.currentTimeMillis()
              val dumpReq = dumpBufferRequest.get
              log.noContext.info(s"Buffer dump requested: $dumpReq")
              // DON'T set pendingBufferDump here - let the conflation handle it
              Right(Some(dumpReq: Respondable))

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
      .via(new BufferWithGlobalDumpStage(maxMessageQueue, 50.milliseconds))
  }

  val resetRequestTimestamps: Flow[TimeSeriesRequest, TimeSeriesRequest, NotUsed] =
    Flow[TimeSeriesRequest]
      .map {
        case tsr if startAtEpoch =>
          tsr.copy(
            startTime = packageMinimumTime + tsr.startTime,
            endTime = packageMinimumTime + tsr.endTime
          )
        case passthrough => passthrough
      }

  val flowGraph: Graph[FlowShape[Message, Message], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // attempt to parse all incoming binary websocket messages,
      // modifying server state where necessary, and filtering out
      // messages that won't requre a response
      val parser = builder.add(parseFlow)

      // dictate where to send different requests types
      val respondablePartition = builder.add(new RespondablePartition)
      val parseErrorsOut = respondablePartition.out0
      val montageRequestOut = respondablePartition.out1
      val timeSeriesRequestOut = respondablePartition.out2

      // reset all timestamps in the incoming TimeSeriesRequest
      val requestTimestampResetter = builder.add(resetRequestTimestamps)

      // query for timeseries data and respond with a datastream given
      // a timeseriers request
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
      parser.out  ~>  respondablePartition.in

      timeSeriesRequestOut ~> requestTimestampResetter ~> queryer ~> responseTimestampResetter ~> wsMessageConverter ~> merge
      montageRequestOut ~> montageLister ~> merge
      parseErrorsOut ~> errorConverter ~> merge
      // format: on

      FlowShape(parser.in, merge.out)

    }

}
