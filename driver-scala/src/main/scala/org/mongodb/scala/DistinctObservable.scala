/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala

import com.mongodb.annotations.{ Alpha, Reason }

import java.util.concurrent.TimeUnit
import com.mongodb.reactivestreams.client.DistinctPublisher
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Collation

import scala.concurrent.duration.Duration

/**
 * Observable for distinct
 *
 * @param wrapped the underlying java DistinctObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class DistinctObservable[TResult](private val wrapped: DistinctPublisher[TResult]) extends Observable[TResult] {

  /**
   * Sets the query filter to apply to the query.
   *
   * [[https://www.mongodb.com/docs/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter, which may be null.
   * @return this
   */
  def filter(filter: Bson): DistinctObservable[TResult] = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): DistinctObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the collation options
   *
   * @param collation the collation options to use
   * @return this
   * @since 1.2
   * @note A null value represents the server default.
   * @note Requires MongoDB 3.4 or greater
   */
  def collation(collation: Collation): DistinctObservable[TResult] = {
    wrapped.collation(collation)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): DistinctObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 4.6
   * @note Requires MongoDB 4.4 or greater
   */
  def comment(comment: String): DistinctObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 4.6
   * @note Requires MongoDB 4.4 or greater
   */
  def comment(comment: BsonValue): DistinctObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the hint for this operation. A null value means no hint is set.
   *
   * @param hint the hint
   * @return this
   * @note if [[hint]] is set that will be used instead of any hint string.
   * @since 5.3
   */
  def hint(hint: Bson): DistinctObservable[TResult] = {
    wrapped.hint(hint)
    this
  }

  /**
   * Sets the hint for this operation. A null value means no hint is set.
   *
   * @param hint the name of the index which should be used for the operation
   * @return this
   * @since 5.3
   */
  def hintString(hint: String): DistinctObservable[TResult] = {
    wrapped.hintString(hint)
    this
  }

  /**
   * Sets the timeoutMode for the cursor.
   *
   * Requires the `timeout` to be set, either in the [[MongoClientSettings]],
   * via [[MongoDatabase]] or via [[MongoCollection]]
   *
   * @param timeoutMode the timeout mode
   * @return this
   * @since 5.2
   */
  @Alpha(Array(Reason.CLIENT))
  def timeoutMode(timeoutMode: TimeoutMode): DistinctObservable[TResult] = {
    wrapped.timeoutMode(timeoutMode)
    this
  }

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   * @since 4.0
   */
  def first(): SingleObservable[TResult] = wrapped.first()

  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
