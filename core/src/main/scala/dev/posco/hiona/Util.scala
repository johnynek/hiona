package dev.posco.hiona

import cats.effect.IO
import java.util.{concurrent => juc}
import java.util.function.BiFunction

object Util {

  /**
    * convert an action that returns a future to a cats IO
    */
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
