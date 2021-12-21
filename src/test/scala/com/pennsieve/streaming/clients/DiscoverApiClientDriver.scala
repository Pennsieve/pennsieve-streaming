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

package com.pennsieve.streaming.clients

import akka.actor.ActorSystem

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext }

object DiscoverApiClientDriver {

  implicit val system: ActorSystem = ActorSystem("discover-api-client-driver")
  implicit val ec: ExecutionContext = system.dispatcher

  val host: String = "https://api.pennsieve.net/discover"

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "provide a package id")
    val client: DiscoverApiClient = new DiscoverApiClientImpl(host, HttpClient())

    val result = client
      .getOrganizationId(args(0))
      .value
      .map {
        case Right(orgId) => println(orgId)
        case Left(error) => error.printStackTrace()
      }
      .map(_ => system.terminate())

    Await.ready(result, Duration.Inf)

  }
}
