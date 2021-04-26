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

package com.pennsieve.streaming.query.chunker

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }

import scala.collection.mutable

/**
  * Created by jsnavely on 5/15/17.
  */
// This stage will accumulate items from a stream until the specified predicate is true.
// it will then emit a chunk, and then continue accumulating until the predicate is true again.
// in our case, we use this stage to chunk up lists of timestamps until they span a certain amount of time

class PredicateStreamChunker[A](predicate: (Seq[A], A) => Boolean)
    extends GraphStage[FlowShape[A, Vector[A]]] {
  val in = Inlet[A]("Chunker.in")
  val out = Outlet[Vector[A]]("Chunker.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val buffer: mutable.Builder[A, Vector[A]] = Vector.newBuilder[A]

      setHandlers(
        in,
        out,
        new InHandler with OutHandler {

          override def onPush(): Unit = {
            val elem = grab(in)
            if (!conditionalEmit(buffer.result(), elem)) {
              pull(in)
            }
            buffer += elem
          }

          private def conditionalEmit(result: Vector[A], latest: A): Boolean = {
            if (predicate(result, latest)) {
              push(out, result)
              buffer.clear()
              true
            } else {
              false
            }
          }

          override def onPull(): Unit = {
            pull(in)
          }

          override def onUpstreamFinish(): Unit = {
            val result = buffer.result()
            if (result.nonEmpty) {
              emit(out, result)
              buffer.clear()
            }
            completeStage()
          }

        }
      )

      override def postStop(): Unit = {
        buffer.clear()
      }

    }

}
