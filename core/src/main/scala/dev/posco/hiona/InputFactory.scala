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

import cats.ApplicativeError
import cats.data.{NonEmptyList, Validated}
import cats.effect.Sync
import fs2.Stream
import java.nio.file.Path

import cats.implicits._

trait InputFactory[F[_]] { self =>
  implicit def canRaise: fs2.RaiseThrowable[F]

  def apply[A](
      src: Event.Source[A],
      offset: Duration
  ): List[Stream[F, Point]]

  private def build(
      inputs: Map[String, (Set[Event.Source[_]], Set[Duration])]
  ): Stream[F, Point] =
    NonEmptyList.fromList(inputs.toList) match {
      case Some(nel) =>
        val checked = nel
          .sortBy(_._1)
          .traverse {
            case (_, (sources, offsets)) =>
              if (sources.size == 1) Validated.valid((sources.head, offsets))
              else
                Validated.invalid(
                  NonEmptyList.fromListUnsafe(sources.toList.sortBy(_.name))
                )
          }
          .leftMap(InputFactory.DuplicatedNames(_))
          .andThen { events =>
            events
              .traverse {
                case (ev, offsets) =>
                  val streams: List[List[Stream[F, Point]]] =
                    offsets.toList.sorted.traverse(apply(ev, _))

                  // each source event must result in at least one stream
                  NonEmptyList.fromList(streams.flatten) match {
                    case Some(nel) => Validated.valid(nel)
                    case None      => Validated.invalidNel(ev)
                  }
              }
              .leftMap(InputFactory.UnknownSources(_))
          }

        checked match {
          case Validated.Valid(streams) =>
            val allStreams: NonEmptyList[Stream[F, Point]] =
              streams.flatten

            Fs2Tools.sortMerge(allStreams.toList)
          case Validated.Invalid(ife) => Stream.raiseError(ife)
        }

      case None => Stream.empty
    }

  final def allInputs[E[_]: Emittable, A](ev: E[A]): Stream[F, Point] =
    build(Emittable[E].sourcesAndOffsetsOf(ev))

  def through(fn: fs2.Pipe[F, Point, Point]): InputFactory[F] =
    new InputFactory[F] {
      def canRaise = self.canRaise

      def apply[A](
          src: Event.Source[A],
          offset: Duration
      ): List[Stream[F, Point]] = self.apply(src, offset).map(fn)
    }

  def combine(that: InputFactory[F]): InputFactory[F] =
    InputFactory.merge(List(this, that))
}

object InputFactory {
  sealed abstract class InputFactoryException(msg: String)
      extends Exception(msg)
  case class UnknownSources(srcs: NonEmptyList[Event.Source[_]])
      extends InputFactoryException(s"unknown sources: $srcs")
  case class DuplicatedNames(srcs: NonEmptyList[Event.Source[_]])
      extends InputFactoryException(s"duplicated name in sources: $srcs")

  def empty[F[_]](implicit rt: fs2.RaiseThrowable[F]): InputFactory[F] =
    new InputFactory[F] {
      def canRaise = rt
      def apply[A](
          src: Event.Source[A],
          offset: Duration
      ): List[Stream[F, Point]] = Nil
    }

  def merge[F[_]](
      factories: Iterable[InputFactory[F]]
  )(implicit rt: fs2.RaiseThrowable[F]): InputFactory[F] =
    new InputFactory[F] {
      def canRaise = rt
      def apply[A](
          src: Event.Source[A],
          offset: Duration
      ): List[Stream[F, Point]] =
        factories.toList.flatMap(_.apply(src, offset))
    }

  def fromMany[F[_]: fs2.RaiseThrowable, E[_]: Emittable, A, B](
      paths: Iterable[(String, B)],
      ev: E[A]
  )(fn: (Event.Source[_], B) => InputFactory[F]): InputFactory[F] = {

    val evMap = Emittable[E].sourcesAndOffsetsOf(ev)

    val factories: List[InputFactory[F]] =
      paths.groupBy(_._1).toList.flatMap {
        case (name, namePaths) =>
          evMap.get(name) match {
            case None           => Nil
            case Some((evs, _)) =>
              // we have duplicate events, this will be caught later
              val ev0 = evs.head
              namePaths.toList.map {
                case (_, path) =>
                  fn(ev0, path)
              }
          }
      }

    merge(factories)
  }

  def fromPaths[F[_]: Sync, E[_]: Emittable, A](
      paths: Iterable[(String, Path)],
      ev: E[A]
  ): InputFactory[F] =
    fromMany[F, E, A, Path](paths, ev) { (ev0, path) =>
      fromPath(ev0, path)
    }

  def fromPath[F[_]: Sync, T](
      ev: Event.Source[T],
      path: Path
  ): InputFactory[F] =
    fromStream(
      ev,
      Fs2Tools.fromPath[F](path, 1 << 16).through(ev.codec.decode)
    )

  def fromStream[F[_], T](ev: Event.Source[T], stream: Stream[F, T])(implicit
      ae: ApplicativeError[F, Throwable]
  ): InputFactory[F] = {
    val cr = implicitly[fs2.RaiseThrowable[F]]
    new InputFactory[F] {
      def canRaise = cr

      def apply[A](
          src: Event.Source[A],
          offset: Duration
      ): List[Stream[F, Point]] =
        if (src == ev) {
          val toPoints = Point.toPoints[F, T](ev, offset)
          toPoints(stream) :: Nil
        } else Nil
    }
  }
}
