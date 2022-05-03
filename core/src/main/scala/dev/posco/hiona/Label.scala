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

import cats.Monoid

import cats.implicits._

/**
  * A Label is something we want to predict, generally about the future.
  * It is time varying, it is basically a future value of a Feature
  */
sealed abstract class Label[K, V] {
  final def lookForward(duration: Duration): Label[K, V] =
    Label.LookForward(this, duration)

  final def zip[W](that: Label[K, W]): Label[K, (V, W)] =
    Label.Zipped(this, that, implicitly[(V, W) =:= (V, W)])

  final def map[W](fn: V => W): Label[K, W] =
    mapWithKey(Feature.ValueMap(fn))

  final def mapWithKey[W](fn: (K, V) => W): Label[K, W] =
    Label.Mapped(this, fn)
}

object Label {
  def apply[K, V](feature: Feature[K, V]): Label[K, V] =
    FromFeature(feature)

  case class FromFeature[K, V](feature: Feature[K, V]) extends Label[K, V]
  case class LookForward[K, V](label: Label[K, V], duration: Duration)
      extends Label[K, V]
  case class Mapped[K, V, W](initial: Label[K, V], fn: (K, V) => W)
      extends Label[K, W]
  case class Zipped[K, X, Y, Z](
      left: Label[K, X],
      right: Label[K, Y],
      cast: (X, Y) =:= Z
  ) extends Label[K, Z]

  def sourcesAndOffsetsOf[K, V](
      label: Label[K, V],
      offset: Duration
  ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
    label match {
      case FromFeature(f) =>
        Feature
          .sourcesOf(f)
          .iterator
          .map { case (n, s) => (n, (s, Set(offset))) }
          .toMap
      case LookForward(l, off1) => sourcesAndOffsetsOf(l, offset + off1)
      case Mapped(l, _)         => sourcesAndOffsetsOf(l, offset)
      case Zipped(l, r, _) =>
        Monoid[Map[String, (Set[Event.Source[_]], Set[Duration])]]
          .combine(
            sourcesAndOffsetsOf(l, offset),
            sourcesAndOffsetsOf(r, offset)
          )
    }

  def lookupsOf[K, V](label: Label[K, V]): Set[Event.Lookup[_, _, _]] =
    label match {
      case FromFeature(f)    => Feature.lookupsOf(f)
      case LookForward(l, _) => lookupsOf(l)
      case Mapped(l, _)      => lookupsOf(l)
      case Zipped(l, r, _)   => lookupsOf(l) | lookupsOf(r)
    }
}
