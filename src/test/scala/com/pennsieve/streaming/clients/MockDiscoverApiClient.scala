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
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.streaming.server.TimeSeriesException

import scala.concurrent.{ ExecutionContext, Future }

class MockDiscoverApiClient(implicit ec: ExecutionContext) extends DiscoverApiClient {

  val defaultOrgId = 11

  val defaultResponse: Right[TimeSeriesException, Int] = Right(defaultOrgId)

  var response: Either[TimeSeriesException, Int] = defaultResponse

  def setResponse(expectedResponse: Either[TimeSeriesException, Int]): Unit = {
    response = expectedResponse
  }

  def resetResponse(): Unit = {
    response = defaultResponse
  }

  override def getOrganizationId(packageId: String): EitherT[Future, TimeSeriesException, Int] =
    EitherT.fromEither(response)
}
