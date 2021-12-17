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

import akka.http.scaladsl.model.StatusCodes
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.models.PackageType.TimeSeries
import com.pennsieve.streaming.clients.HttpClient.HttpClient
import io.circe.syntax.EncoderOps
import io.circe.{ Decoder, DecodingFailure, Encoder, ObjectEncoder }

import scala.concurrent.{ ExecutionContext, Future }

sealed trait OrganizationIdResponse

case class Id(organizationId: Int) extends OrganizationIdResponse
case class NotFound() extends OrganizationIdResponse
case class NotTimeSeries() extends OrganizationIdResponse
case class Error(message: HttpError) extends OrganizationIdResponse

trait DiscoverApiClient {

  def getFileTreePage(
    packageId: String,
    offset: Int = 0,
    limit: Int = 100
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, FileTreePage]

  def getOrganizationId(
    packageId: String
  )(implicit
    ec: ExecutionContext
  ): Future[OrganizationIdResponse] =
    getFileTreePage(packageId).fold(
      e => if (e.statusCode == StatusCodes.NotFound) NotFound() else Error(e),
      p =>
        if (p.files
            .exists(f => f.isInstanceOf[File] && f.asInstanceOf[File].packageType == TimeSeries))
          Id(p.organizationId)
        else
          NotTimeSeries()
    )
}

class DiscoverApiClientImpl(host: String, httpClient: HttpClient) extends DiscoverApiClient {

  override def getFileTreePage(
    packageId: String,
    offset: Int,
    limit: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, FileTreePage] = ???

}

trait FileTreeNodeDTO {
  def name: String
  def path: String
  def size: Long
}
object FileTreeNodeDTO {
  val discriminator: String = "type"
  implicit val encoder: Encoder[FileTreeNodeDTO] = Encoder.instance({
    case e: File =>
      e.asJsonObject.add(discriminator, "File".asJson).asJson
    case e: Directory =>
      e.asJsonObject.add(discriminator, "Directory".asJson).asJson
  })
  implicit val decoder: Decoder[FileTreeNodeDTO] = Decoder.instance { c =>
    val discriminatorCursor = c.downField(discriminator)
    discriminatorCursor
      .as[String]
      .flatMap({
        case "File" =>
          c.as[File]
        case "Directory" =>
          c.as[Directory]
        case tpe =>
          Left(
            DecodingFailure(
              "Unknown value " ++ tpe ++ " (valid: File, Directory)",
              discriminatorCursor.history
            )
          )
      })
  }
}

case class File(
  name: String,
  path: String,
  size: Long,
  icon: com.pennsieve.models.Icon,
  uri: String,
  fileType: com.pennsieve.models.FileType,
  packageType: com.pennsieve.models.PackageType,
  sourcePackageId: Option[String] = None,
  createdAt: Option[java.time.OffsetDateTime] = None
) extends FileTreeNodeDTO
object File {
  implicit val encodeFile: ObjectEncoder[File] = {
    val readOnlyKeys = Set[String]()
    Encoder
      .forProduct9(
        "name",
        "path",
        "size",
        "icon",
        "uri",
        "fileType",
        "packageType",
        "sourcePackageId",
        "createdAt"
      ) { (o: File) =>
        (
          o.name,
          o.path,
          o.size,
          o.icon,
          o.uri,
          o.fileType,
          o.packageType,
          o.sourcePackageId,
          o.createdAt
        )
      }
      .mapJsonObject(_.filterKeys(key => !(readOnlyKeys contains key)))
  }
  implicit val decodeFile: Decoder[File] = Decoder.forProduct9(
    "name",
    "path",
    "size",
    "icon",
    "uri",
    "fileType",
    "packageType",
    "sourcePackageId",
    "createdAt"
  )(File.apply)
}

case class Directory(name: String, path: String, size: Long) extends FileTreeNodeDTO
object Directory {
  implicit val encodeDirectory: ObjectEncoder[Directory] = {
    val readOnlyKeys = Set[String]()
    Encoder
      .forProduct3("name", "path", "size") { (o: Directory) =>
        (o.name, o.path, o.size)
      }
      .mapJsonObject(_.filterKeys(key => !(readOnlyKeys contains key)))
  }
  implicit val decodeDirectory: Decoder[Directory] =
    Decoder.forProduct3("name", "path", "size")(Directory.apply)
}

case class FileTreePage(
  limit: Int,
  offset: Int,
  totalCount: Long,
  files: Seq[FileTreeNodeDTO] = Seq.empty,
  organizationId: Int
)

object FileTreePage {
  implicit val encodeFileTreeWithOrgPage: ObjectEncoder[FileTreePage] = {
    val readOnlyKeys = Set[String]()
    Encoder
      .forProduct5("limit", "offset", "totalCount", "files", "organizationId") {
        (o: FileTreePage) =>
          (o.limit, o.offset, o.totalCount, o.files, o.organizationId)
      }
      .mapJsonObject(_.filterKeys(key => !(readOnlyKeys contains key)))
  }
  implicit val decodeFileTreeWithOrgPage: Decoder[FileTreePage] =
    Decoder.forProduct5("limit", "offset", "totalCount", "files", "organizationId")(
      FileTreePage.apply
    )
}
