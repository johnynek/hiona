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
import com.monovore.decline.Command
import scala.jdk.CollectionConverters._

object IOEnv {
  val read: IO[Map[String, String]] =
    IO(System.getenv().asScala.toMap)

  /** Read args strictly from the Opts.env args */
  def readArgs[A](cmd: Command[A]): IO[A] = {

    def result[B](e: Either[B, A]): IO[A] =
      e match {
        case Right(a) => IO.pure(a)
        case Left(err) =>
          IO.defer {
            System.err.println(err.toString)
            IO.raiseError(new Exception(s"could not parse args: $err"))
          }
      }

    for {
      env <- read
      either = cmd.parse(Nil, env)
      r <- result(either)
    } yield r
  }

}
