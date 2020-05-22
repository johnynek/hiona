package dev.posco.hiona.aws

import cats.data.NonEmptyList
import io.circe.{Encoder, Json}

/**
  * A model of composed AWS lambda functions
  * Pua is hawaiian for "arrow", which is
  * the category theory name for something
  * that composes like this
  *
  * Json => IO[Json]
  * some a (encoded as json) => IO[b (encoded as json)]
  */
sealed abstract class Pua {
  import Pua._

  def andThen(p: Pua): Pua =
    Compose(this, p)

  def optimize: Pua =
    Pua.optimize(this)

  // run a number of copies of a Pua in parallel
  def parCount(count: Int): Pua =
    NonEmptyList.fromList((0 until count).map(_ => this).toList) match {
      case None =>
        throw new IllegalArgumentException(s"count must be > 0, found: $count")
      case Some(nel) => Parallel(nel)
    }
}

object Pua {

  def call(name: String): Pua =
    Call(LambdaFunctionName(name))

  // an AWS lambda function, it has some JSON input and emits some json output
  case class Call(name: LambdaFunctionName) extends Pua

  def const[A: Encoder](a: A): Pua =
    Const(Encoder[A].apply(a))

  // a function that always returns a given json value
  // _ => a
  case class Const(toJson: Json) extends Pua

  // a => a
  case object Identity extends Pua

  // a => IO[b] . b => IO[c] == a => IO[c]
  //
  // def compose(f1, f2):
  //   return lambda x: f2(f1(x))
  case class Compose(first: Pua, second: Pua) extends Pua

  // this converts a list of lambdas:
  // [j => j1, j => j2, ...]
  // into
  // j => [j1, j2, j3, ...]
  // where all the function applications happen in parallel
  //
  // def fanout(lams):
  //   return lambda x: [l(x) for l in lams]
  case class Fanout(lams: NonEmptyList[Pua]) extends Pua

  // [a1 => b1, a2 => b2, .. ]
  // to [a1, a2, a3, ...] => [b1, b2, b3, ...]
  case class Parallel(lams: NonEmptyList[Pua]) extends Pua
  /*
   * We could have a loop construct:
   * a => [null, b] | [a, null]
   * and we keep looping as long as the first item is not null, else return second
   * case class Loop(loopFn: Pua) extends Pua
   */

  // an example of some of the optimizations we can
  // do on this graph to maximize parallelism
  def optimize(p: Pua): Pua = {
    @annotation.tailrec
    def uncompose(front: List[Pua], acc: List[Pua]): List[Pua] =
      front match {
        case Nil                   => acc.reverse
        case Compose(a, b) :: tail => uncompose(a :: b :: tail, acc)
        case Identity :: tail      => uncompose(tail, acc)
        case notComp :: tail       => uncompose(tail, notComp :: acc)
      }

    def optList(ops: List[Pua]): Option[List[Pua]] =
      ops match {
        case Nil | _ :: Nil => None
        case h1 :: (rest1 @ (h2 :: rest2)) =>
          (h1, h2) match {
            case (Parallel(p1), Parallel(p2)) if p1.size == p2.size =>
              val newHead = Parallel(
                p1.zipWith(p2) { case (a, b) => optimize(Compose(a, b)) }
              )
              val better = newHead :: rest2
              optList(better).orElse(Some(better))

            case (Fanout(p1), Parallel(p2)) if p1.size == p2.size =>
              val newHead = Fanout(
                p1.zipWith(p2) { case (a, b) => optimize(Compose(a, b)) }
              )
              val better = newHead :: rest2
              optList(better).orElse(Some(better))
            case _ =>
              // we can't compose the front, but maybe the rest
              optList(rest1).map(h1 :: _)
          }
      }

    def recompose(list: List[Pua]): Pua =
      list match {
        case Nil       => Identity
        case h :: Nil  => h
        case h :: rest => Compose(h, recompose(rest))
      }

    val uncomposed = uncompose(p :: Nil, Nil)

    optList(uncomposed) match {
      case None    => recompose(uncomposed)
      case Some(o) => recompose(o)
    }
  }
}
