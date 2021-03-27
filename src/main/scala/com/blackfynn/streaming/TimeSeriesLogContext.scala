// Copyright (c) [2018] - [2019] Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.streaming

import com.blackfynn.service.utilities.LogContext

final case class TimeSeriesLogContext(
  organizationId: Option[Int] = None,
  userId: Option[Int] = None,
  packageId: Option[Int] = None
) extends LogContext { self =>
  def withPackageId(packageId: Int): TimeSeriesLogContext =
    self.copy(packageId = Some(packageId))

  override val values: Map[String, String] = inferValues(this)
}
