package dev.posco.hiona.aws

import cats.{Applicative, Monad}
import io.circe.{Encoder, Json}

import cats.implicits._

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
  def andThen(p: Pua): Pua =
    Pua.Compose(this, p)

  def optimize: Pua =
    Pua.optimize(this)
}

object Pua {

  def call(name: String): Pua =
    Call(LambdaName(name))

  case class LambdaName(asString: String)

  // an AWS lambda function, it has some JSON input and emits some json output
  case class Call(name: LambdaName) extends Pua

  def const[A: Encoder](a: A): Pua =
    Const(Encoder[A].apply(a))

  // run a number of copies of a Pua in parallel
  def parCount(count: Int, p: Pua): Pua = {
    require(count > 0, s"count must be > 0, found: $count")

    Parallel((0 until count).map(_ => p).toList)
  }

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
  case class Fanout(lams: List[Pua]) extends Pua

  // [a1 => b1, a2 => b2, .. ]
  // to [a1, a2, a3, ...] => [b1, b2, b3, ...]
  case class Parallel(lams: List[Pua]) extends Pua
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
                p1.zip(p2).map { case (a, b) => optimize(Compose(a, b)) }
              )
              val better = newHead :: rest2
              optList(better).orElse(Some(better))

            case (Fanout(p1), Parallel(p2)) if p1.size == p2.size =>
              val newHead = Fanout(
                p1.zip(p2).map { case (a, b) => optimize(Compose(a, b)) }
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

abstract class SlotEnv {

  // some ID in a data-base we can use
  // to find a pointer to result data
  type SlotId

  /**
    * This tracks a value A and the set of opened
    * slots
    */
  type Slots[A]
  implicit def applicativeSlots: Applicative[Slots]

  type Eff[A]
  implicit def monadEff: Monad[Eff]

  // run the lambda as an event-like function with a given
  // location to put the result
  // this is also responsible for updating the state of the db
  // if the arg slot is not yet ready, we update the list of
  // nodes waiting on the arg
  def toFnLater(ln: Pua.LambdaName, arg: SlotId, out: SlotId): Eff[Unit]

  // We have to wait for inputs to be ready for constant
  // functions because we need to order effects correctl;
  def toConst(json: Json, arg: SlotId, out: SlotId): Eff[Unit]

  // This creates a new entry in the database for an output location
  // only one item should ever write to it
  def allocSlot: Slots[SlotId]

  // this is really a special job just waits for all the inputs
  // and finally writes to an output
  def makeList(inputs: List[SlotId], output: SlotId): Eff[Unit]

  // opposite of makeList, wait on a slot, then unlist it
  def unList(input: SlotId, outputs: List[SlotId]): Eff[Unit]

  def start(p: Pua): Slots[SlotId => Eff[SlotId]] = {
    import Pua._

    p match {
      case Const(toJson) =>
        // we have to wait on a slot since functions
        // are effectful
        allocSlot.map(res => {s: SlotId => toConst(toJson, s, res).as(res)})
      case Call(nm) =>
        allocSlot.map(out => {arg: SlotId => toFnLater(nm, arg, out).as(out)})
      case Compose(f, s) =>
        (start(f), start(s)).mapN { (fstart, sstart) => arg: SlotId =>
          for {
            s1 <- fstart(arg)
            s2 <- sstart(s1)
          } yield s2
        }

      case Fanout(lams) =>
        (lams.traverse(start), allocSlot).mapN {
          (innerFns, res) => arg: SlotId =>
            for {
              inners <- innerFns.traverse(_(arg))
              _ <- makeList(inners, res)
            } yield res
        }
      case Identity =>
        applicativeSlots.pure { s: SlotId => monadEff.pure(s) }
      case Parallel(pars) =>
        // [a1 => b1, a2 => b2, .. ]
        // to [a1, a2, a3, ...] => [b1, b2, b3, ...]
        (pars.traverse(start), pars.traverse(_ => allocSlot), allocSlot).mapN {
          (innerFns, inSlots, res) => arg: SlotId =>
            for {
              _ <- unList(arg, inSlots)
              outSlots <- innerFns.zip(inSlots).traverse { case (f, s) => f(s) }
              _ <- makeList(outSlots, res)
            } yield res
        }
    }
  }

}
