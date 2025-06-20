package com.pennsieve.streaming.server

import akka.actor.ActorSystem
import com.pennsieve.models.Channel
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.{Event, RangeLookUp, Segment, TimeSeriesMessage, UnitRangeLookUp}
import com.pennsieve.streaming.query.{RangeRequest, UnitRangeRequest, WsClient}
import com.pennsieve.streaming.server.TimeSeriesFlow.{SessionFilters, SessionKillSwitches, SessionMontage, WithError}
import com.typesafe.config.Config
import scalikejdbc.DBSession

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

class TestTimeSeriesFlow(
                             session: String,
                             sessionFilters: SessionFilters,
                             sessionMontage: SessionMontage,
                             sessionKillSwitches: SessionKillSwitches,
                             channelMap: Map[String, Channel],
                             rangeLookup: RangeLookUp,
                             unitRangeLookUp: UnitRangeLookUp,
                             startAtEpoch: Boolean = false,
                             delayMs: Int = 50  // Configurable delay
                           )(implicit
                             log: ContextLogger,
                             dbSession: DBSession,
                             ec: ExecutionContext,
                             system: ActorSystem,
                             config: Config,
                             wsClient: WsClient
                           ) extends TimeSeriesFlow(
  session,
  sessionFilters,
  sessionMontage,
  sessionKillSwitches,
  channelMap,
  rangeLookup,
  unitRangeLookUp,
  startAtEpoch
) {

  // Helper to create mock TimeSeriesMessage responses
  private def createMockTimeSeriesMessage(channelId: String = "test_channel"): TimeSeriesMessage = {
    TimeSeriesMessage(
      segment = Some(Segment(
        startTs = System.currentTimeMillis(),
        source = channelId,
        pageStart = 0L,
        pageEnd = 1000L,
        segmentType = "Continuous",
        requestedSamplePeriod = 1000,
        channelName = "Test Channel",
        data = Vector.fill(100)(scala.util.Random.nextDouble()), // Mock data
        nrPoints = 100
      )),
      totalResponses = 1,
      responseSequenceId = 0
    )
  }

  private def createMockUnitMessage(channelId: String = "test_unit_channel"): TimeSeriesMessage = {
    TimeSeriesMessage(
      event = Some(Event(
        pageStart = 0L,
        pageEnd = 1000L,
        source = channelId,
        samplePeriod = 1000,
        times = Vector(100L, 1L, 200L, 1L, 300L, 1L), // Mock event times
        data = Vector(1.0, 2.0, 3.0) // Mock event data
      )),
      totalResponses = 1,
      responseSequenceId = 0
    )
  }

  // Override processRequest for continuous data
  override def processRequest(
                               request: WithError[(RangeRequest, Option[RangeRequest])],
                               messageTime: Long
                             ): Future[Option[WithError[TimeSeriesMessage]]] = {

    log.noContext.info(s"DelayedTimeSeriesFlow: Starting processRequest with ${delayMs}ms delay")

    request.fold(
      // Handle error case
      e => {
        akka.pattern.after(delayMs.milliseconds, system.scheduler) {
          Future.successful(Some(Left(e)))
        }
      },
      // Handle success case
      { case (leadChannel, secondaryChannel) =>
        // Simulate the delay that would occur during real S3/HTTP queries
        akka.pattern.after(delayMs.milliseconds, system.scheduler) {
          try {
            // Create a mock response based on the request
            val mockMessage = createMockTimeSeriesMessage(leadChannel.channelNodeId)

            log.noContext.info(s"DelayedTimeSeriesFlow: Completed processRequest after ${delayMs}ms delay")
            Future.successful(Some(Right(mockMessage)))

          } catch {
            case ex: Exception =>
              log.noContext.error(s"DelayedTimeSeriesFlow: Error in processRequest", ex)
              Future.successful(Some(Left(TimeSeriesException.UnexpectedError(ex.getMessage))))
          }
        }
      }
    )
  }

  // Override processUnitRequest for unit/event data
  override def processUnitRequest(
                                   request: WithError[UnitRangeRequest],
                                   messageTime: Long
                                 ): Future[Option[WithError[TimeSeriesMessage]]] = {

    log.noContext.info(s"DelayedTimeSeriesFlow: Starting processUnitRequest with ${delayMs}ms delay")

    request.fold(
      // Handle error case
      e => {
        akka.pattern.after(delayMs.milliseconds, system.scheduler) {
          Future.successful(Some(Left(e)))
        }
      },
      // Handle success case
      rr => {
        // Simulate the delay that would occur during real S3/HTTP queries
        akka.pattern.after(delayMs.milliseconds, system.scheduler) {
          try {
            // Create a mock response based on the request
            val mockMessage = createMockUnitMessage(rr.channel)

            log.noContext.info(s"DelayedTimeSeriesFlow: Completed processUnitRequest after ${delayMs}ms delay")
            Future.successful(Some(Right(mockMessage)))

          } catch {
            case ex: Exception =>
              log.noContext.error(s"DelayedTimeSeriesFlow: Error in processUnitRequest", ex)
              Future.successful(Some(Left(TimeSeriesException.UnexpectedError(ex.getMessage))))
          }
        }
      }
    )
  }
}