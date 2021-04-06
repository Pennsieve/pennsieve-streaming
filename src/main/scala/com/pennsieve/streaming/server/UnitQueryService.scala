package com.pennsieve.streaming.server

import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.scaladsl.Source
import cats.implicits._
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.{ overLimit, parseLong }
import de.knutwalker.akka.http.support.CirceHttpSupport._
import io.circe.Encoder

import scala.util.Success
import scala.util.Failure
import com.pennsieve.streaming.query.{ QuerySequencer, UnitRangeRequest }
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT
import com.blackfynn.streaming.UnitRangeEntry

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

class UnitQueryService(
  querySequencer: QuerySequencer,
  queryLimit: Long,
  maybeClaim: Option[Claim]
)(implicit
  ports: WebServerPorts,
  ec: ExecutionContext
) extends Directives
    with TSJsonSupport {

  implicit val jsonStreamingSupport =
    EntityStreamingSupport.json()
  implicitly[Encoder[Long]]

  def route: Route = path("unit") {
    parameter('session, 'start, 'end, 'channel, 'limit) {
      (session, start, end, channelNodeId, limit) =>
        {
          get {
            val unitQuery: WithErrorT[Source[Long, Any]] =
              for {
                channelAndLogContext <- {
                  ports.getChannelByNodeId(session, channelNodeId, maybeClaim)
                }
                (channel, logContext) = channelAndLogContext

                startL <- parseLong(start)
                  .leftMap(TimeSeriesException.fromCoreError)
                endL <- parseLong(end)
                  .leftMap(TimeSeriesException.fromCoreError)
                _ <- overLimit(startL, endL, channel.rate, queryLimit)
                  .toEitherT[Future]
                  .leftMap(TimeSeriesException.fromCoreError)
                limitL = Try(limit.toLong).toOption
                emptyLookup = UnitRangeEntry(
                  0,
                  min = 0,
                  max = 0,
                  channel = "",
                  count = 0,
                  tsindex = "",
                  tsblob = ""
                )

                ur = UnitRangeRequest(
                  channel.nodeId,
                  startL,
                  endL,
                  limitL,
                  pixelWidth = 0,
                  spikeDuration = 0,
                  spikeDataPointCount = 0,
                  sampleRate = 0.0,
                  lookUp = emptyLookup,
                  totalRequests = None,
                  sequenceId = None
                )

                qresult <- querySequencer
                  .unitRangeRequestT(ur)
                  .leftMap(TimeSeriesException.fromCoreError)

              } yield qresult

            onComplete(unitQuery.value) {
              case Success(either) =>
                either.fold(e => complete { e.statusCode -> e }, data => complete { data })
              case Failure(unexpected) => {
                unexpected.printStackTrace()
                val error =
                  TimeSeriesException.UnexpectedError(unexpected.toString)
                complete {
                  error.statusCode -> error
                }
              }
            }
          }
        }
    }
  }

}
