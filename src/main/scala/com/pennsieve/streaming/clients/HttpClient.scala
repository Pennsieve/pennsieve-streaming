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

// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.streaming.clients

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, StatusCode }
import akka.stream.Materializer
import cats.data.EitherT
import cats.implicits._

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

case class HttpError(statusCode: StatusCode, body: String) extends Throwable {
  override def getMessage: String = s"HTTP $statusCode: $body"
}

object HttpClient {
  type HttpClient =
    HttpRequest => EitherT[Future, HttpError, String]

  def sendHttpRequest(
    req: HttpRequest
  )(implicit
    system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext
  ): EitherT[Future, HttpError, String] =
    EitherT[Future, HttpError, String] {
      for {
        resp <- Http().singleRequest(req)
        body <- resp.entity.toStrict(5.seconds)
      } yield
        if (resp.status.isSuccess()) body.data.utf8String.asRight
        else HttpError(resp.status, body.data.utf8String).asLeft
    }

  def apply(
  )(implicit
    system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext
  ): HttpClient =
    sendHttpRequest(_)
}
