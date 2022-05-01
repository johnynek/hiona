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

import scala.collection.mutable.{Map => MMap}

sealed abstract class Point {
  def ts: Timestamp
  def offset: Duration
  def name: String
  def key: Point.Key
}

object Point {

  /**
    * like a case class, but we cache hashCode because these
    * are used as hashkeys, and we very rarely allocate them
    * (we cache the keys themselves inside the Feeders)
    */
  final class Key private (
      val name: String,
      val offset: Duration,
      override val hashCode: Int
  ) {
    override def toString = s"Key($name, $offset)"
  }

  object Key {
    private var nextHashCode: Int = 1
    private val cache: MMap[(String, Duration), Key] =
      MMap()

    def apply(name: String, offset: Duration): Key =
      // this is okay to be a bit slow because we almost
      // never allocate these, and never in a hot-loop
      cache.synchronized {
        cache.getOrElseUpdate(
          (name, offset), {
            val hc = nextHashCode
            // multiply by a prime to make a non-repeating
            // cycle of hashcodes
            nextHashCode = nextHashCode * 61
            new Key(name, offset, hc)
          }
        )
      }
  }

  case class Sourced[A](
      src: Event.Source[A],
      value: A,
      ts: Timestamp,
      key: Key
  ) extends Point {
    def offset: Duration = key.offset
    def name: String = src.name
  }

  implicit val orderingForPoint: Ordering[Point] =
    new Ordering[Point] {
      def compare(lpoint: Point, rpoint: Point) = {
        val res = Timestamp.compareDiff(
          lpoint.ts,
          lpoint.offset,
          rpoint.ts,
          rpoint.offset
        )
        if (res == 0) {
          // if two points happen at the same time, the one with
          // the greater duration, is picked to come first.
          // so we reverse the ordering here
          // the motivation is so look-aheads should always come
          // before the events that read them if they happen at the same
          // adjusted time
          val cmpDur =
            java.lang.Long.compare(rpoint.offset.millis, lpoint.offset.millis)
          if (cmpDur == 0) lpoint.name.compare(rpoint.name)
          else cmpDur
        } else res
      }
    }

  /** convert validated A values to Points */
  def toPoints[F[_], A](src: Event.Source[A], offset: Duration)(implicit
      ae: cats.ApplicativeError[F, Throwable]
  ): fs2.Pipe[F, A, Point] = {
    // allocate the key once, this is important since they are cached
    val key = Key(src.name, offset)
    val validator = src.validator

    { in: fs2.Stream[F, A] =>
      in.evalMapChunk { a =>
        validator.validate(a) match {
          case Right(ts) => ae.pure(Sourced[A](src, a, ts, key): Point)
          case Left(err) => ae.raiseError[Point](err)
        }
      }
    }
  }
}
