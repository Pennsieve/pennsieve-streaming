/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.pennsieve.streaming.server

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.headers.{ HttpOrigin, HttpOriginRange }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.ExecutionDirectives.handleRejections
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.ActorMaterializer
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.streaming.query.S3WsClient
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import scalikejdbc.AutoSession
import scalikejdbc.config.DBsWithEnv

import scala.concurrent.duration._

object Boot extends App {

  implicit val log: ContextLogger = new ContextLogger()
  implicit val config: Config = ConfigFactory.load()

  DBsWithEnv("postgresTS").setupAll()

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()

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
