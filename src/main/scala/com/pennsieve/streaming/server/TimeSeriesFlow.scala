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
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.ThrottleMode.Shaping
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition}
import akka.stream.{FlowShape, Graph, KillSwitches, SharedKillSwitch}
import akka.util.ByteString
import cats.data.EitherT
import cats.implicits._

import java.util.concurrent.atomic.AtomicLong
import com.pennsieve.models.Channel
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query._
import com.pennsieve.streaming.server.StreamUtils.{EitherOptionFilter, splitMerge}
import com.pennsieve.streaming.server.TSJsonSupport._
import com.pennsieve.streaming.server.TimeSeriesFlow.{SessionFilters, SessionKillSwitches, SessionMontage, WithError}
import com.pennsieve.streaming.{RangeLookUp, TimeSeriesMessage, UnitRangeLookUp}
import com.typesafe.config.Config
import scalikejdbc.DBSession
import spray.json._
import uk.me.berndporr.iirj.Butterworth

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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

  // Atomic flags for thread-safe coordination
  private val lastAbortTime = new AtomicLong(-1L)

  // Helper to check if a message timestamp is still valid (after last abort)
  private def isValidMessage(messageTime: Long, message: String): Boolean = {
    val abortTime = lastAbortTime.get()

    log.noContext.debug(s"From $message}")
    if (abortTime == -1L || messageTime == 0L) {
      // No abort has happened yet OR no timestamp available - allow all messages
      log.noContext.debug(s"DEBUG: Allowing message - has_abortTime=${abortTime != -1L}, has_messageTime=${messageTime != 0L}")

      true
    } else {
      val isValid = messageTime > abortTime
      if (isValid) {
        log.noContext.debug(s"DEBUG: PASS Message validation - $messageTime > $abortTime = $isValid")
      } else {
        log.noContext.debug(s"DEBUG: BLOCK Message validation - $messageTime > $abortTime = $isValid")
      }

      messageTime > abortTime
    }
  }

  val unitDataMultiFlow: Flow[WithError[Respondable], WithError[(UnitRangeRequest, Long)], NotUsed] =
    Flow[WithError[Respondable]]
      .collect {
        case Right(TimestampedRequest(tsr: TimeSeriesRequest, timestamp)) => (tsr, timestamp)
      }
      .filter { case (_, timestamp) =>
        if (!isValidMessage(timestamp, "unitDataMultiFlow")) {
          log.noContext.debug("Filtering out unit data request due to abort")
          false
        } else {
          true
        }
      }
      .map { case (tsr, timestamp) =>
        tsr.channelIds.flatMap { channelIds =>
          val missingChannels = channelIds.toSet -- channelMap.keySet
          if (missingChannels.isEmpty)
            Right(
              channelIds
                .map(channelMap)
                .flatMap(tsr.toUnitRangeRequests(unitRangeLookUp))
                .map(request => (request, timestamp))
            )
          else
            Left(
              TimeSeriesException
                .PackageMissingChannels(missingChannels.toList, tsr.packageId)
            )
        }
      }
      .mapConcat(_.fold(e => List(Left(e)), requests => requests.map(Right(_))))

  val timeSeriesMultiFlow: Flow[WithError[Respondable], WithError[((RangeRequest, Option[RangeRequest]), Long)], NotUsed] =
    Flow[WithError[Respondable]]
      .collect {
        case Right(TimestampedRequest(tsr: TimeSeriesRequest, timestamp)) => (tsr, timestamp)
      }
      .filter { case (_, timestamp) =>
        if (!isValidMessage(timestamp, "timeseriesFlow")) {
          log.noContext.debug("Filtering out time series request due to abort")
          false
        } else {
          true
        }
      }
      .map { case (tsr, timestamp) =>
        for {
          virtualChannels <- tsr.getVirtualChannels(channelMap)
          montagedChannels <- Montage.buildMontage(
            packageMontages,
            tsr.packageId,
            channelMap,
            virtualChannels
          )
          alignedRequests <- montagedChannels.flatMap {
            case (leadChannel, maybeSecondaryChannel) => {
              val leadChannelRequests =
                tsr.toContinuousRangeRequests(rangeLookup)(leadChannel)

              maybeSecondaryChannel
                .map(tsr.toContinuousRangeRequests(rangeLookup)) match {
                case Some(secondaryChannelRequests) => {
                  if (leadChannelRequests.length != secondaryChannelRequests.length)
                    List(
                      Left(
                        TimeSeriesException
                          .MontageMisalignment(leadChannel.nodeId, maybeSecondaryChannel.get.nodeId)
                      )
                    )
                  else
                    leadChannelRequests
                      .sortBy(_.sequenceId)
                      .zip(secondaryChannelRequests.sortBy(_.sequenceId))
                      .map {
                        case (l, s) =>
                          Right(((l, Some(s)), timestamp))
                      }
                }
                case None =>
                  leadChannelRequests.map(r => Right(((r, None), timestamp)))
              }
            }
          }.sequence
        } yield alignedRequests
      }
      .mapConcat(_.fold(e => List(Left(e)), requests => requests.map(Right(_))))

  val queryUnitHttpS3ExecFlow
  : Flow[WithError[(UnitRangeRequest, Long)], WithError[(TimeSeriesMessage, Long)], NotUsed] =
    Flow[WithError[(UnitRangeRequest, Long)]]
      .filter(_.fold(_ => true, { case (rr, _) => channelTypeMatch(rr.channel, "unit", channelMap) }))
      .mapAsyncUnordered(parallelism) {
        case Left(error) => Future.successful(Some(Left(error)))
        case Right((request, messageTime)) =>
          if (!isValidMessage(messageTime, "queryUnitHttpS3ExecFlow")) {
            log.noContext.debug(s"ABORT: Dropping unit query with timestamp $messageTime")
            Future.successful(None)
          } else {
            processUnitRequest(Right(request), messageTime).map {
              case Some(Right(message)) => Some(Right((message, messageTime)))
              case Some(Left(error)) => Some(Left(error))
              case None => None
            }
          }
      }
      .collect { case Some(result) => result }


  // Helper method for unit requests
  protected def processUnitRequest(
                                  request: WithError[UnitRangeRequest], // Remove the tuple, just pass the request
                                  messageTime: Long
                                ): Future[Option[WithError[TimeSeriesMessage]]] = {
    request.fold(
      e => Future.successful(Some(Left(e))),
      rr => {
        val queryFuture = unitQueryExecutor.rangeQuery(rr)
        queryFuture
          .map { result =>
            Some(Right(result))
          }
          .recover {
            case ex =>
              Some(Left(TimeSeriesException.UnexpectedError(ex.getMessage)))
          }
      }
    )
  }

  val queryHttpS3ExecFlow
  : Flow[WithError[((RangeRequest, Option[RangeRequest]), Long)], WithError[(TimeSeriesMessage, Long)], NotUsed] =
    Flow[WithError[((RangeRequest, Option[RangeRequest]), Long)]]
      .filter(_.fold(_ => true, { case (channels, _) =>
        val allChannelIds = channels._1.channelNodeId :: channels._2.map(_.channelNodeId).toList
        allChannelIds.forall(channel => channelTypeMatch(channel, "continuous", channelMap))
      }))
      .mapAsyncUnordered(parallelism) {
        case Left(error) => Future.successful(Some(Left(error)))
        case Right((channels, messageTime)) =>
          if (!isValidMessage(messageTime, "queryHttpS3ExecFlow")) {
            log.noContext.debug(s"ABORT: Dropping query with timestamp $messageTime")
            Future.successful(None)
          } else {
            processRequest(Right(channels), messageTime).map {
              case Some(Right(message)) => Some(Right((message, messageTime)))
              case Some(Left(error)) => Some(Left(error))
              case None => None
            }
          }
      }
      .collect { case Some(result) => result }

  // Helper method to avoid complex nesting
  protected def processRequest(
                              request: WithError[(RangeRequest, Option[RangeRequest])], // Remove tuple wrapping
                              messageTime: Long
                            ): Future[Option[WithError[TimeSeriesMessage]]] = {
    request.fold(
      e => Future.successful(Some(Left(e))), {
        case (leadChannel, secondaryChannel) =>
          val queryFuture =
            continuousQueryExecutor.rangeQuery(leadChannel, channelFilters, secondaryChannel)

          queryFuture
            .map { result =>
              Some(Right(result))
            }
            .recover {
              case ex =>
                Some(Left(TimeSeriesException.UnexpectedError(ex.getMessage)))
            }
      }
    )
  }


  val dualQueryExecutor: Graph[FlowShape[WithError[Respondable], WithError[(TimeSeriesMessage, Long)]], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val partition = builder.add(
        Partition[WithError[Respondable]](2, {
          case Right(TimestampedRequest(tsr: TimeSeriesRequest, _)) =>
            val hasUnitChannels = tsr.channelIds.exists(channelIds =>
              channelIds.exists(channelId => channelTypeMatch(channelId, "unit", channelMap))
            )
            if (hasUnitChannels) 0 else 1
          case _ => 1
        })
      )

      val merge = builder.add(Merge[WithError[(TimeSeriesMessage, Long)]](2))

      val unitData = builder.add(unitDataMultiFlow)
      val unitQuery = builder.add(queryUnitHttpS3ExecFlow)
      val timeSeriesData = builder.add(timeSeriesMultiFlow)
      val timeSeriesQuery = builder.add(queryHttpS3ExecFlow)

      partition.out(0) ~> unitData ~> unitQuery ~> merge.in(0)      // Unit path
      partition.out(1) ~> timeSeriesData ~> timeSeriesQuery ~> merge.in(1)  // Continuous path

      FlowShape(partition.in, merge.out)
    }

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
                      case Seq(time, count) => Seq(time - packageMinimumTime, count)
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

  // UPDATED parseFlow with timestamp tagging
  def parseFlow: Flow[Message, WithError[Respondable], NotUsed] = {
    Flow[Message]
      .throttle(throttleItems, throttlePeriod.second, throttleItems, Shaping)
      .via(killswitch.flow)
      .keepAlive(15.seconds, () => TextMessage.Strict(new KeepAlive().toJson.toString))
      .mapAsync(1) {
        case textMessage: TextMessage =>
          textMessage.toStrict(10 seconds).map { message =>
            val messageTimestamp = System.currentTimeMillis()
            val timeSeriesRequest = Try(message.text.parseJson.convertTo[TimeSeriesRequest])
            val montageRequest = Try(message.text.parseJson.convertTo[MontageRequest])
            val dumpBufferRequest = Try(message.text.parseJson.convertTo[DumpBufferRequest])

            if (dumpBufferRequest.isSuccess) {

              lastActive = System.currentTimeMillis()

              // Record abort time - this invalidates ALL previous messages
              val abortTime = System.currentTimeMillis()
              lastAbortTime.set(abortTime)
              log.noContext.info(s"ABORT: Invalidating all messages before timestamp $abortTime")

              Right(None)

            } else if (timeSeriesRequest.isSuccess) {
              lastActive = System.currentTimeMillis()

              // Check if this message is still valid (after any abort)
//              if (isValidMessage(messageTimestamp,"parseFlow")) {
                // Store timestamp in thread-local for pipeline stages to check
                val timestamped = TimestampedRequest(timeSeriesRequest.get, messageTimestamp)
                Right(Some(timestamped: Respondable))
//              } else {
//                log.noContext
//                  .debug(s"ABORT: Dropping TimeSeriesRequest with timestamp $messageTimestamp")
//                Right(None)
//              }

            } else if (montageRequest.isSuccess) {
              lastActive = System.currentTimeMillis()

              // Check if this message is still valid (after any abort)
//              if (isValidMessage(messageTimestamp,"parseFlow")) {
                // Store timestamp in thread-local for pipeline stages to check
                val timestamped = TimestampedRequest(montageRequest.get, messageTimestamp)
                Right(Some(timestamped: Respondable))
//              } else {
//                log.noContext
//                  .debug(s"ABORT: Dropping MontageRequest with timestamp $messageTimestamp")
//                Right(None)
//              }

            } else {
              perform(message.text) match {
                case Success(_) => Right(None)
                case Failure(_) =>
                  Left(
                    TimeSeriesException.UnexpectedError(s"unsupported message received: $message")
                  )
              }
            }
          }
        case _: BinaryMessage =>
          Future.successful(
            Left(TimeSeriesException.UnexpectedError("received unexpected binary message"))
          )
      }
      .via(EitherOptionFilter)
  }

  // Helper to extract timestamp from requests
  private def extractTimestamp(request: WithError[Respondable]): Long = {
    request match {
      case Right(TimestampedRequest(_, timestamp)) =>
        log.noContext.debug(s"DEBUG: Extracted timestamp $timestamp from TimestampedRequest")
        if (timestamp == 0L) {
          log.noContext.warn(s"DEBUG: Found 0L timestamp in TimestampedRequest!")
        }
        timestamp
      case other =>
        val currentTime = System.currentTimeMillis()
        log.noContext.debug(s"DEBUG: Using current time $currentTime for non-TimestampedRequest: $other")
        currentTime
    }
  }

  private def extractOriginal(request: WithError[Respondable]): WithError[Respondable] = {
    request match {
      case Right(TimestampedRequest(original, _)) => Right(original)
      case other => other
    }
  }

//  object MessageTimestamp {
//    private val timestamp = new ThreadLocal[Long]()
//
//    def set(time: Long): Unit = timestamp.set(time)
//    def get(): Long = Option(timestamp.get()).getOrElse(0L)
//    def clear(): Unit = timestamp.remove()
//  }

  val resetRequestTimestamps: Flow[TimeSeriesRequest, TimeSeriesRequest, NotUsed] =
    Flow[TimeSeriesRequest]
      .map { tsr =>
        if (startAtEpoch) {
          tsr.copy(
            startTime = packageMinimumTime + tsr.startTime,
            endTime = packageMinimumTime + tsr.endTime
          )
        } else {
          tsr
        }
      }

  val flowGraph: Graph[FlowShape[Message, Message], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val parser = builder.add(parseFlow)
      val timestampProcessor = builder.add(
        Flow[WithError[Respondable]]
          .map { request =>
            val timestamp = extractTimestamp(request)
            if (isValidMessage(timestamp,"flowGraph")) {
              request
            } else {
              log.noContext.debug(s"ABORT: Dropping request with timestamp $timestamp")
              Left(TimeSeriesException.UnexpectedError("DROPPED_BY_ABORT"))
            }
          }
          .filter {
            case Left(TimeSeriesException.UnexpectedError("DROPPED_BY_ABORT", _)) => false
            case _ => true
          }
      )

      // Add the missing broadcast
      val broadcast = builder.add(Broadcast[WithError[Respondable]](2))

      // Add the missing montageFlow
      val montageFlow = builder.add(
        Flow[WithError[Respondable]]
          .collect { case Right(TimestampedRequest(mr: MontageRequest, _)) => mr }
          .via(listMontageChannels)
      )

      val queryer = builder.add(dualQueryExecutor)

      val queryValidation = builder.add(
        Flow[WithError[(TimeSeriesMessage, Long)]]
          .filter {
            case Right((_, messageTime)) =>
              if (!isValidMessage(messageTime, "post-querier")) {
                log.noContext.debug(s"ABORT: Dropping response with timestamp $messageTime")
                false
              } else {
                true
              }
            case Left(_) => true
          }
          .map(_.map(_._1))
      )

      val responseTimestampResetter = builder.add(resetResponseTimestamps)
      val wsMessageConverter = builder.add(toWsMessage)
      val merge = builder.add(Merge[Message](2)) // Changed to 2 since you only have 2 inputs

      // Connections
      parser.out ~> timestampProcessor ~> broadcast.in
      broadcast.out(0) ~> queryer ~> queryValidation ~> responseTimestampResetter ~> wsMessageConverter ~> merge.in(0)
      broadcast.out(1) ~> montageFlow ~> merge.in(1)

      FlowShape(parser.in, merge.out)
    }

}
