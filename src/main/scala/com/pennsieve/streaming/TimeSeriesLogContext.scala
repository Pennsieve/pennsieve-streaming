package com.pennsieve.streaming

import com.pennsieve.service.utilities.LogContext

final case class TimeSeriesLogContext(
  organizationId: Option[Int] = None,
  userId: Option[Int] = None,
  packageId: Option[Int] = None
) extends LogContext { self =>
  def withPackageId(packageId: Int): TimeSeriesLogContext =
    self.copy(packageId = Some(packageId))

  override val values: Map[String, String] = inferValues(this)
}
