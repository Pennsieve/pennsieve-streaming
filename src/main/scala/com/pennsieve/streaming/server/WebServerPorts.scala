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

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.auth.middleware.{ Jwt, ServiceClaim, UserClaim }
import com.pennsieve.core.utilities.{ InsecureCoreContainer, JwtAuthenticator, SecureCoreContainer }
import com.pennsieve.models.{ Channel, Organization, Package, User }
import com.pennsieve.streaming.TimeSeriesLogContext
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT
import com.pennsieve.streaming.server.containers.{ InsecureAWSContainer, SecureAWSContainer }
import com.typesafe.config.Config

import scala.concurrent.{ ExecutionContext, Future }

/**
  * The WebServer requires information about channels, packages and
  * sessions, this trait provides access to that information
  */
trait WebServerPorts {
  def getChannels(
    packageNodeId: String,
    claim: Jwt.Claim
  ): WithErrorT[(List[Channel], TimeSeriesLogContext)]

  def getChannelByNodeId(
    channelNodeId: String,
    claim: Jwt.Claim
  ): WithErrorT[(Channel, TimeSeriesLogContext)]

  val rangeLookupQuery =
    "select id, location, channel, rate, lower(range) as lo, upper(range) as hi from timeseries.ranges where (channel = {channel}) and (range && int8range({qstart},{qend})) order by lo asc"

  val unitRangeLookupQuery =
    "select id, count, channel, tsindex, tsblob, lower(range) as lo, upper(range) as hi from timeseries.unit_ranges where (channel = {channel}) and (range && int8range({qstart},{qend})) order by lo asc"
}

/**
  * A concrete implementation that retrieves required data from the
  * pennsieve graph and postgres instance using pennsieve-api's
  * secure/insecure containers
  */
class GraphWebServerPorts(
  config: Config
)(implicit
  system: ActorSystem,
  ec: ExecutionContext
) extends WebServerPorts {
  private val insecureContainer =
    new InsecureAWSContainer(config, ec, system) with InsecureCoreContainer

  implicit val jwtConfig: Jwt.Config = getJwtConfig(config)

  /**
    * Get a secure container instance from a JWT.
    *
    * - If given a user claim, verify the user is in the correct role to interact with the organization.
    *
    * - If given a service claim, verify the `X-ORGANIZATION-(INT-)ID` header is present.
    *
    * @return
    */
  private def getSecureContainerFromJwt(claim: Jwt.Claim) =
    claim.content match {
      // case: User
      //   Attempt to extract out a (user, organization) and use that to build a secure container:
      case _: UserClaim =>
        for {
          userContext <- {
            JwtAuthenticator.userContext(insecureContainer, claim)
          }
        } yield {
          val sContainer =
            secureContainerBuilder(userContext.user, userContext.organization, claim.content.roles)
          (
            sContainer,
            TimeSeriesLogContext(
              userId = Some(userContext.user.id),
              organizationId = Some(userContext.organization.id)
            )
          )
        }

      // case: Service
      //   Rejected in the routing logic this should never be called
      case ServiceClaim(_) => ???
    }

  private def secureContainerBuilder(
    user: User,
    org: Organization,
    roleOverrides: List[Jwt.Role]
  ): SecureAWSContainer with SecureCoreContainer =
    new SecureAWSContainer(
      insecureContainer.config,
      insecureContainer.db,
      insecureContainer.redisClientPool,
      org,
      user,
      system.dispatcher,
      system,
      roleOverrides
    ) with SecureCoreContainer

  override def getChannels(
    packageNodeId: String,
    claim: Jwt.Claim
  ): WithErrorT[(List[Channel], TimeSeriesLogContext)] =
    for {
      containerAndLogContext <- getSecureContainerFromJwt(claim)
        .leftMap(TimeSeriesException.fromCoreError)
      (secureContainer, logContext) = containerAndLogContext

      `package` <- {
        secureContainer.packageManager
          .getByNodeId(packageNodeId)
          .leftMap(TimeSeriesException.fromCoreError)
      }: EitherT[Future, TimeSeriesException, Package]

      channels <- {
        secureContainer.timeSeriesManager
          .getChannels(`package`)
          .leftMap(TimeSeriesException.fromCoreError)
      }
    } yield (channels, logContext.withPackageId(`package`.id))

  override def getChannelByNodeId(
    channelNodeId: String,
    claim: Jwt.Claim
  ): WithErrorT[(Channel, TimeSeriesLogContext)] = {
    for {
      containerAndLogContext <- getSecureContainerFromJwt(claim)
        .leftMap(TimeSeriesException.fromCoreError)
      (secureContainer, logContext) = containerAndLogContext
      channel <- secureContainer.timeSeriesManager
        .insecure_getChannelByNodeId(channelNodeId)
        .leftMap(TimeSeriesException.fromCoreError)
    } yield (channel, logContext.withPackageId(channel.packageId))
  }
}
