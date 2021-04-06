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
  ) extends SecureContainer(config, db, redisClientPool, user, organization, roleOverrides)(
        executionContext,
        system
      )
}
