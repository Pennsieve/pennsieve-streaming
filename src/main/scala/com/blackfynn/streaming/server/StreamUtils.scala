package com.blackfynn.streaming.server

import akka.NotUsed
import akka.stream.{ FlowShape, Graph }
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge }

import com.blackfynn.streaming.server.TimeSeriesFlow.WithError

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
