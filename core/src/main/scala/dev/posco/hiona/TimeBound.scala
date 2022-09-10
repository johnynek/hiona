/*
 * Copyright 2022 devposco
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

package dev.posco.hiona

import cats.data.Validated
import com.monovore.decline.Opts
import fs2.Stream

import cats.implicits._

sealed abstract class TimeBound(
    val inclusiveLowerBound: Option[Timestamp],
    val exclusiveUpperBound: Option[Timestamp]
) {
  def apply(ts: Timestamp): Boolean

  // input can drop anything exceeding the upper bound
  def filterInput[F[_], A](fn: A => Timestamp): fs2.Pipe[F, A, A] =
    exclusiveUpperBound match {
      case None => identity
      case Some(ub) =>
        stream: Stream[F, A] => stream.takeWhile(a => fn(a) < ub)
    }

  // at output we can filter lower bounds
  def filterOutput[F[_], A](fn: A => Timestamp): fs2.Pipe[F, A, A] =
    inclusiveLowerBound match {
      case None => identity
      case Some(lb) =>
        stream: Stream[F, A] => stream.dropWhile(a => fn(a) < lb)
    }
}

object TimeBound {
  case object Everything extends TimeBound(None, None) {
    def apply(ts: Timestamp) = true
  }
  case class LowerBounded(lower: Timestamp)
      extends TimeBound(Some(lower), None) {
    def apply(ts: Timestamp) = lower <= ts
  }
  case class UpperBounded(exclusiveUpper: Timestamp)
      extends TimeBound(None, Some(exclusiveUpper)) {
    def apply(ts: Timestamp) = ts < exclusiveUpper
  }
  case class Bounded(lower: Timestamp, exclusiveUpper: Timestamp)
      extends TimeBound(Some(lower), Some(exclusiveUpper)) {
    def apply(ts: Timestamp) = (lower <= ts) && (ts < exclusiveUpper)
  }

  val opts: Opts[TimeBound] =
    (
      Opts
        .option[Long](
          "inclusive_lower_ms",
          "inclusive lower bound of the timestamp in milliseconds to emit"
        )
        .orNone,
      Opts
        .option[Long](
          "exclusive_upper_ms",
          "exclusive upper bound of the timestamp in milliseconds to emit"
        )
        .orNone
    ).mapN {
      case (None, None)    => TimeBound.Everything
      case (Some(l), None) => TimeBound.LowerBounded(Timestamp(l))
      case (None, Some(u)) => TimeBound.UpperBounded(Timestamp(u))
      case (Some(l), Some(u)) =>
        TimeBound.Bounded(Timestamp(l), Timestamp(u))
    }.mapValidated {
      case TimeBound.Bounded(l, u) if !(l < u) =>
        Validated.invalidNel(
          s"inclusive_lower_ms ($l) must be < than exclusive_upper_ms ($u)"
        )
      case good => Validated.valid(good)
    }

}
