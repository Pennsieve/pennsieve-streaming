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

/**
  * Created by jsnavely on 5/8/17.
  */
import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.util.ByteString

// The ByteStringChunkers will apportion a stream of ByteStrings in a new stream of ByteStrings of the specified standard size,
// which is useful for grabbing items of a fixed size
// this is adapted from the akka streams cookbook in the akka documentation:
// http://doc.akka.io/docs/akka/2.5.1/scala/stream/stream-cookbook.html#Chunking_up_a_stream_of_ByteStrings_into_limited_size_ByteStrings

class ByteStringChunker(val chunkSize: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("Chunker.in")
  val out = Outlet[ByteString]("Chunker.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var buffer = ByteString.empty

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (isClosed(in)) {
            emitChunk()
          } else {
            pull(in)
          }
        }
      })
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            buffer ++= elem
            emitChunk()
          }

          override def onUpstreamFinish(): Unit = {
            if (buffer.isEmpty) {
              completeStage()
            } else {
              // There are elements left in buffer, so
              // we keep accepting downstream pulls and push from buffer until emptied.
              //
              // It might be though, that the upstream finished while it was pulled, in which
              // case we will not get an onPull from the downstream, because we already had one.
              // In that case we need to emit from the buffer.
              if (isAvailable(out)) {
                emitChunk()
              }
            }
          }
        }
      )

      private def emitChunk(): Unit = {
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage()
          else pull(in)
        } else {
          val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
          buffer = nextBuffer
          push(out, chunk)
        }
      }

    }
}
