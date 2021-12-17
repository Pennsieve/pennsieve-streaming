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
import com.pennsieve.core.utilities.{ InsecureCoreContainer, JwtAuthenticator }
import com.pennsieve.domain.CoreError
import com.pennsieve.models.{ Channel, Organization, Package, User }
import com.pennsieve.streaming.TimeSeriesLogContext
import com.pennsieve.streaming.clients.{ DiscoverApiClient, DiscoverApiClientImpl, HttpClient }
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT
import com.pennsieve.streaming.server.containers.{
  InsecureAWSContainer,
  OrganizationScopedContainer,
  ScopedContainer,
  SecureAWSContainer
}
import com.typesafe.config.Config

import scala.concurrent.{ ExecutionContext, Future }

/**
  * The WebServer requires information about channels, packages and
  * sessions, this trait provides access to that information
  */
trait WebServerPorts {
  def getChannels(
    packageNodeId: String,
    claim: Jwt.Claim,
    packageOrgId: Option[Int] = None
  ): WithErrorT[(List[Channel], TimeSeriesLogContext)]

  def getChannelByNodeId(
    channelNodeId: String,
    claim: Jwt.Claim
  ): WithErrorT[(Channel, TimeSeriesLogContext)]

  def discoverApiClient: DiscoverApiClient

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

  override val discoverApiClient =
    new DiscoverApiClientImpl(config.getString("discover-api.host"), HttpClient())

  private def lookupOrg(orgId: Option[Int]): EitherT[Future, CoreError, Option[Organization]] = {
    if (orgId.isDefined)
      insecureContainer.organizationManager.get(orgId.get).map(Some(_))
    else
      EitherT.rightT(None)

  }

  /**
    * Get a secure container instance from a JWT.
    *
    * - If given a user claim, verify the user is in the correct role to interact with the organization.
    *
    * - If given a service claim, verify the `X-ORGANIZATION-(INT-)ID` header is present.
    *
    * @return
    */
  private def getSecureContainerFromJwt(claim: Jwt.Claim, packageOrgId: Option[Int]) =
    claim.content match {
      // case: User
      //   Attempt to extract out a (user, organization) and use that to build a secure container:
      case _: UserClaim =>
        for {
          userContext <- {
            JwtAuthenticator.userContext(insecureContainer, claim)
          }
          packageOrg <- lookupOrg(packageOrgId)

        } yield {
          val sContainer =
            scopedContainerBuilder(userContext.user, userContext.organization, packageOrg)
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

  private def scopedContainerBuilder(
    user: User,
    userOrg: Organization,
    packageOrgOpt: Option[Organization]
  ): ScopedContainer = {
    packageOrgOpt match {
      case None => secureContainerBuilder(user, userOrg)
      case Some(packageOrg) =>
        new OrganizationScopedContainer(
          insecureContainer.config,
          insecureContainer.db,
          packageOrg,
          system.dispatcher,
          system
        )

    }
  }

  private def secureContainerBuilder(user: User, org: Organization): ScopedContainer =
    new SecureAWSContainer(
      insecureContainer.config,
      insecureContainer.db,
      org,
      user,
      system.dispatcher,
      system
    )

  override def getChannels(
    packageNodeId: String,
    claim: Jwt.Claim,
    packageOrgId: Option[Int]
  ): WithErrorT[(List[Channel], TimeSeriesLogContext)] =
    for {
      containerAndLogContext <- getSecureContainerFromJwt(claim, packageOrgId)
        .leftMap(TimeSeriesException.fromCoreError)
      (secureContainer, logContext) = containerAndLogContext

      pennsievePackage <- {
        secureContainer
          .getPackageByNodeId(packageNodeId)
          .leftMap(TimeSeriesException.fromCoreError)
      }: EitherT[Future, TimeSeriesException, Package]

      channels <- {
        secureContainer.timeSeriesManager
          .getChannels(pennsievePackage)
          .leftMap(TimeSeriesException.fromCoreError)
      }
    } yield (channels, logContext.withPackageId(pennsievePackage.id))

  override def getChannelByNodeId(
    channelNodeId: String,
    claim: Jwt.Claim
  ): WithErrorT[(Channel, TimeSeriesLogContext)] = {
    for {
      containerAndLogContext <- getSecureContainerFromJwt(claim, None)
        .leftMap(TimeSeriesException.fromCoreError)
      (secureContainer, logContext) = containerAndLogContext
      channel <- secureContainer.timeSeriesManager
        .insecure_getChannelByNodeId(channelNodeId)
        .leftMap(TimeSeriesException.fromCoreError)
    } yield (channel, logContext.withPackageId(channel.packageId))
  }
}
