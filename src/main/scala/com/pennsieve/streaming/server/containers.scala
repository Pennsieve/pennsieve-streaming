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
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.models.{ Organization, User }
import com.pennsieve.traits.PostgresProfile.api._
import com.pennsieve.core.utilities.{ InsecureContainer, RedisContainer, SecureContainer }
import com.typesafe.config.Config
import com.redis.RedisClientPool

import scala.concurrent.ExecutionContext

object containers {

  abstract class InsecureAWSContainer(
    config: Config,
    executionContext: ExecutionContext,
    system: ActorSystem
  ) extends InsecureContainer(config)(executionContext, system)
      with RedisContainer

  abstract class SecureAWSContainer(
    config: Config,
    db: Database,
    redisClientPool: RedisClientPool,
    organization: Organization,
    user: User,
    executionContext: ExecutionContext,
    system: ActorSystem,
    roleOverrides: List[Jwt.Role]
  ) extends SecureContainer(config, db, user, organization)(executionContext, system)
}
