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

import akka.stream.stage.{ GraphStage, GraphStageLogic }
import akka.stream._
import com.pennsieve.streaming.server.TimeSeriesFlow.WithError

/** An Akka stream element that will partition the given Inlet into
  * different outlets - one for each implementer of `Respondable` plus
  * one for errors.
  */
class RespondablePartition
    extends GraphStage[
      FanOutShape3[WithError[Respondable], TimeSeriesException, MontageRequest, TimeSeriesRequest]
    ] {
  private val name = "RespondablePartition"

  override val shape =
    new FanOutShape3[WithError[Respondable], TimeSeriesException, MontageRequest, TimeSeriesRequest](
      name
    )

  val in: Inlet[WithError[Respondable]] = shape.in
  val errorsOut: Outlet[TimeSeriesException] = shape.out0
  val montageRequestOut: Outlet[MontageRequest] = shape.out1
  val timeSeriesRequestOut: Outlet[TimeSeriesRequest] = shape.out2

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private def bothAvailable: Boolean =
        isAvailable(montageRequestOut) && isAvailable(timeSeriesRequestOut)

      private def attemptPull(): Unit = {
        if (bothAvailable && isAvailable(in)) grabAndPush()
        else if (bothAvailable && !hasBeenPulled(in)) pull(in)
      }

      private def grabAndPush(): Unit =
        grab(in) match {
          case Right(mr: MontageRequest) => push(montageRequestOut, mr)
          case Right(tsr: TimeSeriesRequest) => push(timeSeriesRequestOut, tsr)
          case Left(tse: TimeSeriesException) => push(errorsOut, tse)
        }

      setHandler(in = in, handler = () => {
        if (bothAvailable) grabAndPush()
      })

      setHandler(out = montageRequestOut, handler = () => attemptPull())
      setHandler(out = timeSeriesRequestOut, handler = () => attemptPull())
      setHandler(out = errorsOut, handler = () => attemptPull())
    }
}
