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

import akka.NotUsed
import akka.stream.{ FlowShape, Graph }
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge }

import com.pennsieve.streaming.server.TimeSeriesFlow.WithError

/**
  * Created by jsnavely on 5/3/17.
  */
object StreamUtils {

  // combines two flows of the same type into a single flow
  def splitMerge[IN, OUT](
    f1: Flow[IN, OUT, NotUsed],
    f2: Flow[IN, OUT, NotUsed]
  ): Graph[FlowShape[IN, OUT], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val split = builder.add(Broadcast[IN](2))
      val merge = builder.add(Merge[OUT](2))
      val _f1 = builder.add(f1)
      val _f2 = builder.add(f2)
      split.out(0) ~> _f1 ~> merge
      split.out(1) ~> _f2 ~> merge
      FlowShape(split.in, merge.out)
    }

  // takes a flow of type Option[T] and filters out the None elements, returning a flow of T
  def OptionFilter[T]: Flow[Option[T], T, NotUsed] =
    Flow[Option[T]]
      .filter(m => m.isDefined) //this awkwardness is because flows don't have a flatMap function
      .map(m => m.get)

  // takes a flow of type Either[Error, Option[T]] and filters out the
  // None elements from the right side, allowing all Lefts through
  def EitherOptionFilter[T]: Flow[WithError[Option[T]], WithError[T], NotUsed] =
    Flow[WithError[Option[T]]]
      .filter(_.fold(_ => true, m => m.isDefined))
      .map(_.map(_.get))

}
