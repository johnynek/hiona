package dev.posco.hiona.db

import cats.{MonadError, Monoid}
import dev.posco.hiona._
import doobie._
import doobie.implicits._

object DBSupport {

  sealed abstract class Factory {
    def build[F[_]](xa: Transactor[F])(
        implicit me: MonadError[F, Throwable]
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

  def factoryFor[A: Read](src: Event.Source[A], sqlString: Fragment): Factory =
    new Factory { self =>
      def build[F[_]](
          xa: Transactor[F]
      )(implicit me: MonadError[F, Throwable]): InputFactory[F] =
        inputFactory[F, A](src, sqlString, xa)
    }

  def inputFactory[F[_], A: Read](
      src: Event.Source[A],
      sqlString: Fragment,
      xa: Transactor[F]
  )(implicit me: MonadError[F, Throwable]): InputFactory[F] =
    InputFactory.fromStream(src, sqlString.query[A].stream.transact(xa))
}
