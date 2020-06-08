package dev.posco.hiona.aws

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import io.circe.{ACursor, Decoder, DecodingFailure, Encoder, HCursor, Json}

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
  import Pua._

  def andThen(p: Pua): Pua =
    Compose(this, p)

  def optimize: Pua =
    Pua.optimize(this)

  // run a number of copies of a Pua in parallel
  // the result is a json list of the same length as count
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

  object Expr {
    sealed abstract class PuaExpr

    case class Let(bindings: Map[String, PuaExpr], in: PuaExpr) extends PuaExpr
    case class Var(name: String) extends PuaExpr
    case class Call(nm: LambdaFunctionName) extends PuaExpr
    case class Const(toJson: Json) extends PuaExpr
    case object Identity extends PuaExpr
    case class Compose(items: NonEmptyList[PuaExpr]) extends PuaExpr
    case class Fanout(lams: NonEmptyList[PuaExpr]) extends PuaExpr
    case class Parallel(lams: NonEmptyList[PuaExpr]) extends PuaExpr

    sealed abstract class Error

    private def toPua(
        expr: PuaExpr,
        scope: Map[String, Pua]
    ): ValidatedNel[String, Pua] = {
      import Validated.valid

      expr match {
        case Let(bs, in) =>
          bs.toList
            .sortBy(_._1)
            .traverse { case (k, v) => toPua(v, scope).map((k, _)) }
            .map(lst => scope ++ lst)
            .andThen(toPua(in, _))
        case Var(nm) =>
          scope.get(nm) match {
            case Some(p) => valid(p)
            case None    => Validated.invalidNel(nm)
          }
        case Const(j) => valid(Pua.Const(j))
        case Call(n)  => valid(Pua.Call(n))
        case Identity => valid(Pua.Identity)
        case Compose(items) =>
          items
            .traverse(toPua(_, scope))
            .map(_.reverse.toList.reduce { (left, right) =>
              Pua.Compose(right, left)
            })
        case Fanout(lams) => lams.traverse(toPua(_, scope)).map(Pua.Fanout(_))
        case Parallel(lams) =>
          lams.traverse(toPua(_, scope)).map(Pua.Parallel(_))
      }
    }

    def toPua(expr: PuaExpr): ValidatedNel[String, Pua] =
      toPua(expr, Map.empty)

    def fromPua(pua: Pua): PuaExpr =
      pua match {
        case Pua.Const(j) => Const(j)
        case Pua.Call(nm) => Call(nm)
        case Pua.Compose(a, b) =>
          Compose(NonEmptyList.of(fromPua(a), fromPua(b)))
        case Pua.Identity       => Identity
        case Pua.Fanout(lams)   => Fanout(lams.map(fromPua))
        case Pua.Parallel(lams) => Parallel(lams.map(fromPua))
      }

    implicit val encoderPuaExpr: Encoder[PuaExpr] =
      new Encoder[PuaExpr] {
        def apply(p: PuaExpr): Json =
          p match {
            case Const(j) =>
              Json.fromValues(List(Json.fromString("const"), j))
            case Call(nm) =>
              Json.fromValues(
                List(Json.fromString("call"), Json.fromString(nm.asString))
              )
            case Compose(items) =>
              Json.fromValues(
                Json.fromString("compose") :: items.map(apply).toList
              )
            case Identity =>
              Json.fromValues(List(Json.fromString("id")))
            case Fanout(outs) =>
              Json.fromValues(
                Json.fromString("fanout") ::
                  outs.toList.map(apply)
              )
            case Parallel(fns) =>
              Json.fromValues(
                Json.fromString("par") ::
                  fns.toList.map(apply)
              )
            case Var(nm) => Json.fromString(nm)
            case Let(binds, in) =>
              Json.fromValues(
                Json.fromString("let") ::
                  Json.obj(binds.toList.sortBy(_._1).map {
                    case (n, p) => (n, apply(p))
                  }: _*) ::
                  apply(in) ::
                  Nil
              )
          }
      }

    implicit val decoderPuaExpr: Decoder[PuaExpr] =
      new Decoder[PuaExpr] {

        def nonEmpty(a: ACursor): Decoder.Result[NonEmptyList[PuaExpr]] = {
          def loop(
              a: ACursor,
              acc: NonEmptyList[Decoder.Result[PuaExpr]]
          ): Decoder.Result[NonEmptyList[PuaExpr]] = {
            val nexta = a.right
            if (nexta.failed) acc.reverse.sequence
            else loop(nexta, nexta.as[PuaExpr] :: acc)
          }

          loop(a, NonEmptyList(a.as[PuaExpr], Nil))
        }

        def apply(hc: HCursor): Decoder.Result[PuaExpr] = {
          val ary = hc.downArray
          if (ary.failed)
            hc.as[String].map(Var(_))
          else onArray(ary)
        }

        def bindings(m: ACursor): Decoder.Result[Map[String, PuaExpr]] =
          m.keys match {
            case None =>
              Left(DecodingFailure("expected to find a {}", m.history))
            case Some(ks) =>
              ks.toList
                .traverse(k => m.get[PuaExpr](k).map((k, _)))
                .map(_.toMap)
          }

        def onArray(ary: ACursor): Decoder.Result[PuaExpr] =
          ary.as[String].flatMap {
            case "const" => ary.right.as[Json].map(Const(_))
            case "call" =>
              ary.right.as[String].map(n => Call(LambdaFunctionName(n)))
            case "compose" => nonEmpty(ary.right).map(Compose(_))
            case "id"      => Right(Identity)
            case "fanout"  => nonEmpty(ary.right).map(Fanout(_))
            case "par"     => nonEmpty(ary.right).map(Parallel(_))
            case "let" =>
              val next = ary.right
              for {
                b <- bindings(next)
                in <- next.right.as[PuaExpr]
              } yield Let(b, in)
            case name => Right(Var(name))
          }
      }
  }

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

  implicit val encoderPua: Encoder[Pua] =
    new Encoder[Pua] {
      def apply(p: Pua): Json =
        p match {
          case Const(j) =>
            Json.fromValues(List(Json.fromString("const"), j))
          case Call(nm) =>
            Json.fromValues(
              List(Json.fromString("call"), Json.fromString(nm.asString))
            )
          case Compose(a, b) =>
            Json.fromValues(
              List(Json.fromString("compose"), apply(a), apply(b))
            )
          case Identity =>
            Json.fromValues(List(Json.fromString("id")))
          case Fanout(outs) =>
            Json.fromValues(
              Json.fromString("fanout") ::
                outs.toList.map(apply)
            )
          case Parallel(fns) =>
            Json.fromValues(
              Json.fromString("par") ::
                fns.toList.map(apply)
            )
        }
    }

  implicit val decoderPua: Decoder[Pua] =
    new Decoder[Pua] {

      def nonEmpty(a: ACursor): Decoder.Result[NonEmptyList[Pua]] = {
        def loop(
            a: ACursor,
            acc: NonEmptyList[Decoder.Result[Pua]]
        ): Decoder.Result[NonEmptyList[Pua]] = {
          val nexta = a.right
          if (nexta.failed) acc.reverse.sequence
          else loop(nexta, nexta.as[Pua] :: acc)
        }

        loop(a, NonEmptyList(a.as[Pua], Nil))
      }

      def apply(hc: HCursor): Decoder.Result[Pua] =
        onArray(hc.downArray)

      def onArray(ary: ACursor): Decoder.Result[Pua] =
        ary.as[String].flatMap {
          case "const" => ary.right.as[Json].map(Const(_))
          case "call"  => ary.right.as[String].map(call(_))
          case "compose" =>
            val ar = ary.right
            for {
              a <- ar.as[Pua]
              b <- ar.right.as[Pua]
            } yield Compose(a, b)
          case "id"     => Right(Identity)
          case "fanout" => nonEmpty(ary.right).map(Fanout(_))
          case "par"    => nonEmpty(ary.right).map(Parallel(_))
        }
    }
}
