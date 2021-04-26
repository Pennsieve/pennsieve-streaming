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

package com.pennsieve.streaming.query

import akka.stream.scaladsl.Source
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream._
import com.pennsieve.service.utilities.ContextLogger

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

class Drain[In, Out](
  kill: KillSwitch,
  drain: Source[In, Any] => Future[Out]
)(implicit
  ec: ExecutionContext,
  log: ContextLogger
) extends GraphStage[FlowShape[Source[In, Any], Source[In, Any]]] {
  val in: Inlet[Source[In, Any]] = Inlet("Drain.in")
  val out: Outlet[Source[In, Any]] = Outlet("Drain.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandlers(
        in,
        out,
        new InHandler with OutHandler {
          override def onPush(): Unit =
            if (!isClosed(out)) push(out, grab(in))
            else if (!isClosed(in)) {
              drain(grab(in)) onComplete {
                case Success(_) =>
                case Failure(e) =>
                  log.noContext.warn(s"Error draining source: $e")
              }
              tryPull(in)
            } else completeStage()
          override def onPull(): Unit = pull(in)

          override def onDownstreamFinish: Unit = kill.shutdown()
        }
      )
    }

}
