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

import uk.me.berndporr.iirj.Cascade

/**
  * Tracks the state of a digital filter cascade and provides filtering operations.
  *
  * @param cascade The underlying IIR filter cascade
  */
class FilterStateTracker(
  private val cascade: Cascade,
  private val filterOrder: Int,
  private val maxFilterFreq: Double
) {

  private var isCleanState: Boolean = true
  private var lastTimestamp: Long = 0L

  /**
    * Applies the filter to the input value and marks the filter as dirty.
    *
    * @param input The input value to filter
    * @return The filtered output value
    */
  def filter(input: Double): Double = {
    isCleanState = false
    cascade.filter(input)
  }

  /**
    * Resets the filter state and marks it as clean.
    */
  def reset(): Unit = {
    cascade.reset()
    isCleanState = true
  }

  /**
    * Checks if the filter is in a clean (unused) state.
    *
    * @return true if the filter hasn't been used since last reset
    */
  def isClean: Boolean = isCleanState

  /**
    * Updates the latest timestamp.
    *
    * @param timestamp The timestamp to set
    */
  def setLatestTimestamp(timestamp: Long): Unit = {
    lastTimestamp = timestamp
  }

  /**
    * Gets the latest recorded timestamp.
    *
    * @return The latest timestamp
    */
  def getLatestTimestamp: Long = lastTimestamp

  /**
    * Gets the current filter order.
    *
    * @return The filter order
    */
  def getFilterOrder: Int = filterOrder

  /**
    * Gets the maximum cutoff frequency.
    *
    * @return The maximum cutoff frequency
    */
  def getMaxFilterFreq: Double = maxFilterFreq

  /**
    * Provides direct access to the underlying cascade filter.
    *
    * @return The cascade filter instance
    */
  def getCascade: Cascade = cascade
}
