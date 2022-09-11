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

package dev.posco.hiona.aws

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import io.circe.jawn.CirceSupportParser.parseFromChannel
import io.circe.{Decoder, Encoder}
import java.io.{InputStream, OutputStream}
import java.nio.channels.Channels

import io.circe.syntax._

abstract class PureLambda[Ctx, A, B](implicit
    val decodeInput: Decoder[A],
    val encodeOutput: Encoder[B]
) extends RequestStreamHandler {

  // this is done once
  def setup: IO[Ctx]

  private lazy val readCtx: Ctx = setup.unsafeRunSync()(IORuntime.global)

  def run(arg: A, ctx: Ctx, context: Context): IO[B]

  final def log(context: Context, msg: => String): IO[Unit] =
    IO(context.getLogger.log(msg))

  def checkContext(ctx: Ctx, context: Context): IO[Unit] = {
    // just use the args
    assert(ctx != null)
    assert(context != null)
    IO.unit
  }

  final def handleRequest(
      inputStream: InputStream,
      outputStream: OutputStream,
      context: Context
  ): Unit = {
    // keep this lazy so lambdas with no input can skip it
    val input: IO[A] =
      IO(Channels.newChannel(inputStream))
        .flatMap(c => IO.defer(IO.fromTry(parseFromChannel(c))))
        .flatMap(_.as[A] match {
          case Right(a)  => IO.pure(a)
          case Left(err) => IO.raiseError(err)
        })

    val ioUnit =
      for {
        _ <- checkContext(readCtx, context)
        a <- input
        b <- run(a, readCtx, context)
        bjsonBytes = b.asJson.noSpaces.getBytes("UTF-8")
        _ <- IO(outputStream.write(bjsonBytes))
      } yield ()

    try ioUnit.unsafeRunSync()(IORuntime.global)
    finally {
      inputStream.close()
      outputStream.close()
    }
  }
}
