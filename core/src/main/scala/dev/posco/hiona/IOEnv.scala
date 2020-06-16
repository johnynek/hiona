package dev.posco.hiona

import cats.effect.IO
import com.monovore.decline.Command
import scala.jdk.CollectionConverters._

object IOEnv {
  val read: IO[Map[String, String]] =
    IO(System.getenv().asScala.toMap)

  /**
    * Read args strictly from the Opts.env args
    */
  def readArgs[A](cmd: Command[A]): IO[A] = {

    def result[B](e: Either[B, A]): IO[A] =
      e match {
        case Right(a) => IO.pure(a)
        case Left(err) =>
          IO.suspend {
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
