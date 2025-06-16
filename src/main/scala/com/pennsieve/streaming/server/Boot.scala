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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.ServerSettings
import akka.actor.ActorSystem
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.S3WsClient
import com.typesafe.config.{ Config, ConfigFactory }
import scalikejdbc.AutoSession
import scalikejdbc.config.DBsWithEnv

import scala.concurrent.duration._

object Boot extends App {

  implicit val log: ContextLogger = new ContextLogger()
  implicit val config: Config = ConfigFactory.load()

  DBsWithEnv("postgresTS").setupAll()

  implicit val system = ActorSystem("system", ConfigFactory.load())

//  implicit val system = ActorSystem("system")

  implicit val dbSession = AutoSession

  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  implicit val wsClient = new S3WsClient(config)
  implicit val webServerPorts = new GraphWebServerPorts(config)

  val api = new WebServer

  val idleTimeout = config.getDuration("timeseries.idle-timeout")

  val serverSettings = ServerSettings(system)

  val routes: Route = Route.seal(api.route)

  // this long idle needs to be twice what is configured so that to custom timeout system can work.
  // otherwise, they both kill it at the same time, and we never have a change to clean up
  val longIdle = FiniteDuration(idleTimeout.toNanos, TimeUnit.NANOSECONDS) + FiniteDuration(
    idleTimeout.toNanos,
    TimeUnit.NANOSECONDS
  )
  serverSettings.withIdleTimeout(longIdle) //lame that this is mutable

  val bindPort = config.getInt("timeseries.bind-port")
  val bindAddress = config.getString("timeseries.bind-address")

  Http()
    .bindAndHandle(routes, bindAddress, bindPort)
    .flatMap { binding =>
      val localAddress = binding.localAddress
      log.noContext.info(
        s"Server online at http://${localAddress.getHostName}:${localAddress.getPort}"
      )

      binding.whenTerminated
        .map { terminated =>
          system.terminate()
          log.noContext
            .info(s"Server has terminated with $terminated")
        }
    }

}
