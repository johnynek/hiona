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

package dev.posco.hiona.db

import cats.{MonadError, Monoid}
import dev.posco.hiona._
import doobie._

import doobie.implicits._

object DBSupport {

  sealed abstract class Factory {
    def build[F[_]](xa: Transactor[F])(implicit
        me: MonadError[F, Throwable]
    ): InputFactory[F]

    def combine(that: Factory): Factory =
      CombineFactory(List(this, that))
  }

  object Factory {
    implicit val factoryMonoid: Monoid[Factory] =
      new Monoid[Factory] {
        val empty = CombineFactory(Nil)
        def combine(left: Factory, right: Factory) =
          CombineFactory(List(left, right))
        override def combineAllOption(
            fs: IterableOnce[Factory]
        ): Option[Factory] =
          if (fs.iterator.isEmpty) None
          else Some(CombineFactory(fs.iterator.toList))
      }
  }

  private case class CombineFactory(facts: List[Factory]) extends Factory {
    def build[F[_]](
        xa: Transactor[F]
    )(implicit me: MonadError[F, Throwable]): InputFactory[F] =
      InputFactory.merge(facts.map(_.build(xa)))
  }

  /**
    * Build a DBSupport.Factory for the given source.
    * There are two input groups to aid in type inference: the first
    * parameter type fixes A, then for query we can infer A. This is
    * useful because the query is generally made with sql"...".query
    *
    * Note, we fix the Query0[A] type rather than accepting a doobie.Fragment
    * to encourage safer reuse of queries (since Fragments are untyped).
    *
    * Finally, doobie logging for an SQL query is controlled by
    * doobie.util.log.LogHandler at the call-site of .query on the fragment
    * By accepting the Query here, we make it clear the logging is controlled
    * outside of this call (in the user code that sets up the source).
    */
  def factoryFor[A](src: Event.Source[A])(query: Query0[A]): Factory =
    new Factory { self =>
      def build[F[_]](
          xa: Transactor[F]
      )(implicit me: MonadError[F, Throwable]): InputFactory[F] =
        inputFactory[F, A](src, query, xa)
    }

  def inputFactory[F[_], A](
      src: Event.Source[A],
      query: Query0[A],
      xa: Transactor[F]
  )(implicit me: MonadError[F, Throwable]): InputFactory[F] =
    InputFactory.fromStream(src, query.stream.transact(xa))
}
