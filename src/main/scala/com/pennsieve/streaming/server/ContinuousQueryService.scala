package com.pennsieve.streaming.server

import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.scaladsl.Source
import cats.implicits._
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.service.utilities.ContextLogger
import com.blackfynn.streaming.LookupResultRow
import com.pennsieve.streaming.query.TimeSeriesQueryUtils.{ overLimit, parseLong }
import com.pennsieve.streaming.query.{ QuerySequencer, RangeRequest }
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class ContinuousQueryService(
  querySequencer: QuerySequencer,
  queryLimit: Long,
  claim: Jwt.Claim
)(implicit
  ports: WebServerPorts,
  ec: ExecutionContext,
  log: ContextLogger
) extends Directives
    with TSJsonSupport {

  implicit val jsonStreamingSupport =
    EntityStreamingSupport.json()

  def route: Route =
    path("continuous") {
      parameter('start, 'end, 'channel, 'limit) { (start, end, channelNodeId, limit) =>
        {
          get {
            val rangeQuery: WithErrorT[Source[(Long, Double), Any]] =
              for {
                channelAndLogContext <- ports.getChannelByNodeId(channelNodeId, claim)
                (channel, logContext) = channelAndLogContext

                startL <- parseLong(start)
                  .leftMap(TimeSeriesException.fromCoreError)
                endL <- parseLong(end)
                  .leftMap(TimeSeriesException.fromCoreError)
                _ <- overLimit(startL, endL, channel.rate, queryLimit)
                  .toEitherT[Future]
                  .leftMap(TimeSeriesException.fromCoreError)
                limitL = Try(limit.toInt).toOption
                emptyLookup = LookupResultRow(-1, startL, endL, channel.rate, channel.nodeId, "")
                qresult <- querySequencer
                  .continuousRangeRequestT(
                    RangeRequest(
                      channel.nodeId,
                      channel.name,
                      startL,
                      endL,
                      limitL,
                      -1,
                      emptyLookup
                    )
                  )
                  .leftMap(TimeSeriesException.fromCoreError)
              } yield qresult

            onComplete(rangeQuery.value) {
              case Success(either) =>
                either.fold(e => complete { e.statusCode -> e }, data => complete { data })
              case Failure(unexpected) => {
                log.noContext.error(unexpected.toString)
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
