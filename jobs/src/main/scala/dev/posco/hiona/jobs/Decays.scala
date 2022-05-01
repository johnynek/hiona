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

package dev.posco.hiona.jobs

import dev.posco.hiona._

import cats.implicits._

/**
  * A Decay computes an exponentially decaying function of its
  * history of inputs. `Decays` enumerates several half-lives.
  * Together, they capture more information about the data, emphasizing
  * recency
  */
case class Decays[A](
    mins10: A,
    hours1: A,
    hours2: A,
    days1: A,
    days3: A,
    days14: A
) {
  def map[B](fn: A => B): Decays[B] =
    Decays(
      mins10 = fn(mins10),
      hours1 = fn(hours1),
      hours2 = fn(hours2),
      days1 = fn(days1),
      days3 = fn(days3),
      days14 = fn(days14)
    )
}

object Decays {
  val mins10: Duration = Duration.minute * 10
  val hours1: Duration = Duration.hour
  val hours2: Duration = Duration.hour * 2
  val days1: Duration = Duration.day
  val days3: Duration = Duration.day * 3
  val days14: Duration = Duration.day * 14

  def feature[K, V: DoubleModule](
      ev: Event[(K, V)]
  ): Feature[K, Decays[V]] = {
    val tupled = ev.valueWithTime
      .mapValues {
        case (v, ts) =>
          (
            Decay.fromTimestamped[mins10.type, V](ts, v),
            Decay.fromTimestamped[hours1.type, V](ts, v),
            Decay.fromTimestamped[hours2.type, V](ts, v),
            Decay.fromTimestamped[days1.type, V](ts, v),
            Decay.fromTimestamped[days3.type, V](ts, v),
            Decay.fromTimestamped[days14.type, V](ts, v)
          )
      }

    val feat = tupled.sum

    feat.mapWithKeyTime {
      case (_, (d1, d2, d3, d4, d5, d6), ts) =>
        Decays(
          d1.atTimestamp(ts),
          d2.atTimestamp(ts),
          d3.atTimestamp(ts),
          d4.atTimestamp(ts),
          d5.atTimestamp(ts),
          d6.atTimestamp(ts)
        )
    }
  }
}
