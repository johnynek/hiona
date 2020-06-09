package dev.posco.hiona.aws

import cats.effect.IO
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

  private lazy val readCtx: Ctx = setup.unsafeRunSync()

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
        .flatMap(c => IO.suspend(IO.fromTry(parseFromChannel(c))))
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

    try ioUnit.unsafeRunSync()
    finally {
      inputStream.close()
      outputStream.close()
    }
  }
}
