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

import com.pennsieve.models.FileType.MEF
import com.pennsieve.models.Icon.Timeseries
import com.pennsieve.models.PackageType.TimeSeries

import java.time.OffsetDateTime
import scala.concurrent.{ ExecutionContext, Future }

class MockDiscoverApiClient extends DiscoverApiClient {

  val defaultFiles: Seq[File] = Seq(
    File(
      name = "channel1.mef",
      path = "files/channel1.mef",
      size = 65003000,
      icon = Timeseries,
      uri = "s3://bucket/1/2/files/channel1.mef",
      fileType = MEF,
      packageType = TimeSeries,
      sourcePackageId = None,
      createdAt = Some(OffsetDateTime.parse("2007-12-03T10:15:30+01:00"))
    )
  )

  val defaultTotalCount: Long = defaultFiles.size
  val defaultOrgId = 1

  case class FileResponse(
    totalCount: Long = defaultTotalCount,
    organizationId: Int = defaultOrgId,
    files: Seq[File] = defaultFiles
  )

  val defaultResponse = Right(FileResponse())

  var response: Either[HttpError, FileResponse] = defaultResponse

  def setResponse(expectedResponse: Either[HttpError, FileResponse]): Unit = {
    response = expectedResponse
  }

  def resetResponse(): Unit = {
    response = defaultResponse
  }

  override def getFileTreePage(
    packageId: String,
    offset: Int,
    limit: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, FileTreePage] = {
    response.fold(EitherT.leftT(_), r => {
      val files = r.files.map(_.copy(sourcePackageId = Some(packageId)))
      EitherT.rightT(FileTreePage(limit, offset, r.totalCount, files, r.organizationId))
    })

  }
}
