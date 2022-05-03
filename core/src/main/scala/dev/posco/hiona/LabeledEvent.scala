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

sealed abstract class LabeledEvent[A] {
  final def map[B](fn: A => B): LabeledEvent[B] =
    LabeledEvent.Mapped(this, fn)

  // we can discard rows if we want to filter before emitting data
  final def filter(fn: A => Boolean): LabeledEvent[A] =
    LabeledEvent.Filtered(this, fn)
}

object LabeledEvent {

  def apply[K, V, W](
      event: Event[(K, V)],
      label: Label[K, W]
  ): LabeledEvent[(K, (V, W))] =
    WithLabel(event, label, implicitly[(K, (V, W)) =:= (K, (V, W))])

  @scala.annotation.tailrec
  def sourcesAndOffsetsOf[A](
      ev: LabeledEvent[A]
  ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
    ev match {
      case WithLabel(ev, label, _) =>
        Monoid[Map[String, (Set[Event.Source[_]], Set[Duration])]]
          .combine(
            Event
              .sourcesOf(ev)
              .iterator
              .map { case (n, s) => (n, (s, Set(Duration.zero))) }
              .toMap,
            Label.sourcesAndOffsetsOf(label, Duration.zero)
          )
      case Mapped(l, _)   => sourcesAndOffsetsOf(l)
      case Filtered(l, _) => sourcesAndOffsetsOf(l)
    }

  def sourcesOf[A](
      ev: LabeledEvent[A]
  ): Map[String, Set[Event.Source[_]]] =
    sourcesAndOffsetsOf(ev).iterator.map { case (k, (v, _)) => (k, v) }.toMap

  @scala.annotation.tailrec
  def lookupsOf[A](le: LabeledEvent[A]): Set[Event.Lookup[_, _, _]] =
    le match {
      case WithLabel(ev, label, _) =>
        Event.lookupsOf(ev) | Label.lookupsOf(label)
      case Mapped(l, _)   => lookupsOf(l)
      case Filtered(l, _) => lookupsOf(l)
    }

  case class WithLabel[K, X, Y, Z](
      event: Event[(K, X)],
      label: Label[K, Y],
      cast: (K, (X, Y)) =:= Z
  ) extends LabeledEvent[Z]
  case class Mapped[A, B](labeled: LabeledEvent[A], fn: A => B)
      extends LabeledEvent[B]
  case class Filtered[A](labeled: LabeledEvent[A], fn: A => Boolean)
      extends LabeledEvent[A]
}
