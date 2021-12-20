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
import com.pennsieve.models.{ Organization, Package, PackageState, User }
import com.pennsieve.traits.PostgresProfile.api._
import com.pennsieve.core.utilities.{
  DatabaseContainer,
  InsecureContainer,
  OrganizationContainer,
  PackagesMapperContainer,
  SecureContainer,
  SecureCoreContainer,
  TimeSeriesManagerContainer
}
import com.pennsieve.domain.{ CoreError, NotFound }
import com.pennsieve.utilities.Container
import com.pennsieve.core.utilities.FutureEitherHelpers.implicits._

import com.typesafe.config.Config

import scala.concurrent.{ ExecutionContext, Future }

object containers {

  abstract class InsecureAWSContainer(
    config: Config,
    executionContext: ExecutionContext,
    system: ActorSystem
  ) extends InsecureContainer(config)(executionContext, system)

  trait ScopedContainer extends Container with TimeSeriesManagerContainer {
    def getPackageByNodeId(nodeId: String): EitherT[Future, CoreError, Package]
  }

  class SecureAWSContainer(
    config: Config,
    db: Database,
    organization: Organization,
    user: User,
    executionContext: ExecutionContext,
    system: ActorSystem
  ) extends SecureContainer(config, db, user, organization)(executionContext, system)
      with ScopedContainer
      with SecureCoreContainer {
    override def getPackageByNodeId(nodeId: String): EitherT[Future, CoreError, Package] =
      packageManager.getByNodeId(nodeId)
  }

  class OrganizationScopedContainer(
    val config: Config,
    _db: Database,
    val organization: Organization,
    implicit
    val ec: ExecutionContext,
    implicit
    val system: ActorSystem
  ) extends ScopedContainer
      with OrganizationContainer
      with DatabaseContainer
      with PackagesMapperContainer {

    override lazy val db: Database = _db

    override def getPackageByNodeId(nodeId: String): EitherT[Future, CoreError, Package] = {
      val query = packagesMapper
        .filter(_.nodeId === nodeId)
        .filter(_.state =!= (PackageState.DELETING: PackageState))
        .result
        .headOption
      db.run(query).whenNone(NotFound(s"Package ($nodeId)"))
    }
  }

}
