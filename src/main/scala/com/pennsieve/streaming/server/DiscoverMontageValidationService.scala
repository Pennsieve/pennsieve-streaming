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

import cats.implicits._
import com.pennsieve.auth.middleware.Jwt.Claim
import com.pennsieve.models.Channel
import com.pennsieve.streaming.TimeSeriesLogContext
import com.pennsieve.streaming.server.TimeSeriesFlow.WithErrorT

import scala.concurrent.ExecutionContext

class DiscoverMontageValidationService(
  claim: Claim
)(implicit
  ports: WebServerPorts,
  ec: ExecutionContext
) extends MontageValidationService(claim)(ports) {

  override def getChannelsQuery(
    packageId: String
  ): WithErrorT[(List[Channel], TimeSeriesLogContext)] =
    for {
      packageOrgId <- ports.discoverApiClient.getOrganizationId(packageId)
      channelsResult <- ports
        .getChannels(packageId, claim, Some(packageOrgId))
    } yield channelsResult

}
