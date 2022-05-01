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

import cats.effect.IO
import java.util.function.BiFunction
import java.util.{concurrent => juc}

object Util {

  /** convert an action that returns a future to a cats IO */
  def fromJava[A](startFuture: () => juc.CompletableFuture[A]): IO[A] =
    IO.cancelable { cb =>
      val cf = startFuture()

      cf.handle[Unit](new BiFunction[A, Throwable, Unit] {
        override def apply(result: A, err: Throwable): Unit =
          err match {
            case null =>
              cb(Right(result))
            case _: juc.CancellationException =>
              ()
            case ex: juc.CompletionException if ex.getCause ne null =>
              cb(Left(ex.getCause))
            case ex =>
              cb(Left(ex))
          }
      })

      IO { cf.cancel(true); () }
    }
}
