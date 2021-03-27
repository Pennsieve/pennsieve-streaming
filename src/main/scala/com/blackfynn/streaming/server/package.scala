package com.blackfynn.streaming

import com.blackfynn.auth.middleware.Jwt
import com.typesafe.config.Config

package object server {
  def getJwtConfig(config: Config): Jwt.Config =
    new Jwt.Config {
      override def key: String = config.getString("jwt-key")
    }
}
