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

import cats.data.{NonEmptyList, ReaderT}
import cats.{Defer, Monad}
import io.circe.Encoder
import org.scalacheck.{Gen, Prop}

import Pua.Expr
import PuaGens.genMonad

class PuaJsonLaws extends munit.ScalaCheckSuite {
  property("we can round-trip puas through json") {
    Prop.forAll(PuaGens.genDirectory.map(_._1)) { pua =>
      val json = Encoder[Pua].apply(pua)

      assertEquals(json.as[Pua], Right(pua))
    }
  }

  test("test some example jsons") {
    def j(p: Pua): String = Encoder[Pua].apply(p).noSpaces

    assertEquals(j(Pua.call("foo")), """["call","foo"]""")
    assertEquals(
      j(Pua.call("foo").andThen(Pua.call("bar"))),
      """["compose",["call","foo"],["call","bar"]]"""
    )
    assertEquals(
      j(Pua.Fanout(NonEmptyList.of(Pua.call("foo"), Pua.call("bar")))),
      """["fanout",["call","foo"],["call","bar"]]"""
    )
    assertEquals(
      j(Pua.Parallel(NonEmptyList.of(Pua.call("foo"), Pua.call("bar")))),
      """["par",["call","foo"],["call","bar"]]"""
    )
    assertEquals(j(Pua.Identity), """["id"]""")
    assertEquals(j(Pua.const(List(1, 2))), """["const",[1,2]]""")
  }

  type S[A] = ReaderT[Gen, Map[String, Expr.PuaExpr], A]

  def oneOf[A](items: Iterable[S[A]]): S[A] = {
    require(items.nonEmpty, s"can't do oneOf(Nil)")
    val vec = items.toVector
    val sz = vec.size
    val idx = ReaderT.liftF(Gen.choose(0, sz - 1)): S[Int]

    idx.flatMap(vec(_))
  }

  def listOfN[A](n: Int, gen: S[A]): S[List[A]] =
    Monad[S].tailRecM((n, List.empty[A])) {
      case (n, acc) if n <= 0 => Monad[S].pure(Right(acc.reverse))
      case (n, acc)           => gen.map(a => Left((n - 1, a :: acc)))
    }

  def genPuaExpr(depth: Int): S[Expr.PuaExpr] = {
    val recur: S[Expr.PuaExpr] = Defer[S].defer(genPuaExpr(depth - 1))

    val get: S[Map[String, Expr.PuaExpr]] = ReaderT.ask

    val genVar: S[Expr.PuaExpr] =
      get.flatMap { items =>
        if (items.isEmpty) recur
        else oneOf(items.map { case (v, _) => Monad[S].pure(Expr.Var(v)) })
      }

    if (depth <= 0)
      oneOf(
        List(
          ReaderT.liftF(Gen.identifier.map { n =>
            Expr.Call(LambdaFunctionName(n))
          }),
          genVar,
          Monad[S].pure(Expr.Identity),
          ReaderT.liftF(PuaAwsActionGens.genJson.map(Expr.Const(_)))
        )
      )
    else {
      val sz = ReaderT.liftF(PuaAwsActionGens.genSizeExp1)
      val genLet: S[Expr.PuaExpr] =
        sz.flatMap(
          listOfN(_, Monad[S].product(ReaderT.liftF(Gen.identifier), recur))
        ).flatMap { lets =>
          val bindings = lets.toMap
          recur
            .local[Map[String, Expr.PuaExpr]](_ ++ bindings)
            .map(Expr.Let(bindings, _))
        }

      def nel(fn: NonEmptyList[Expr.PuaExpr] => Expr.PuaExpr): S[Expr.PuaExpr] =
        sz.flatMap(z => listOfN(z + 1, recur))
          .map {
            case Nil => sys.error("unreachable, size is > 0")
            case h :: t =>
              fn(NonEmptyList(h, t))
          }

      val genComp = nel(Expr.Compose(_))
      val genFan = nel(Expr.Fanout(_))
      val genPar = nel(Expr.Parallel(_))

      oneOf(List(recur, recur, recur, genLet, genComp, genFan, genPar))
    }
  }

  property("pua Expr round-trip") {
    Prop.forAll(genPuaExpr(3).run(Map.empty)) { expr =>
      Expr.toPua(expr).toEither match {
        case Left(_) => assert(false)
        case r @ Right(pua) =>
          val puaJ = Encoder[Pua].apply(pua)
          val expr0 = Expr.fromPua(pua)
          assertEquals(Expr.toPua(expr0).toEither, r)
          puaJ.as[Expr.PuaExpr] match {
            case Right(expr1) => assertEquals(Expr.toPua(expr1).toEither, r)
            case Left(_)      => assert(false)
          }
      }
    }
  }

  test("test some example json pua expr") {
    def check(left: String, right: String) = {
      import io.circe.parser.parse
      val expr = parse(left).flatMap(_.as[Expr.PuaExpr])
      val pua = parse(right).flatMap(_.as[Pua])

      (expr, pua) match {
        case (Right(e), Right(p)) =>
          assertEquals(Expr.toPua(e).toEither, Right(p))
        case err => assert(false, err)
      }
    }

    check("""["call","foo"]""", """["call","foo"]""")
    check("""["let", {"a": ["call","foo"]}, "a"]""", """["call","foo"]""")
    check(
      """["let", {"a": ["call","foo"]}, ["compose", "a"]]""",
      """["call","foo"]"""
    )
    check(
      """["let", {"a": ["call","foo"], "b": ["call", "bar"]}, ["compose", "a", "b"]]""",
      """["compose", ["call","foo"], ["call", "bar"]]"""
    )
  }

}
