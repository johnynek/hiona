package dev.posco.hiona.aws

import cats.{Defer, Monad}
import cats.data.{NonEmptyList, StateT}
import cats.effect.IO
import org.scalacheck.{Arbitrary, Cogen, Gen, Prop}
import io.circe.Json

import Arbitrary.{arbitrary => arb}
import cats.implicits._

object PuaGens {
  // gen and cogen for a certain shape of json:
  case class JsonShape(gen: Gen[Json], cogen: Cogen[Json]) {
    def optional: JsonShape =
      // null | a
      JsonShape(
        Gen.oneOf(Gen.const(Json.Null), gen),
        Cogen { (seed: org.scalacheck.rng.Seed, json: Json) =>
          json match {
            case Json.Null => seed.next
            case notNull   => cogen.perturb(seed, notNull)
          }
        }
      )

    def tuple2(that: JsonShape): JsonShape =
      JsonShape(
        Gen.zip(gen, that.gen).map {
          case (j1, j2) => Json.fromValues(List(j1, j2))
        },
        Cogen.tuple2(cogen, that.cogen).contramap[Json] { j =>
          j.asArray match {
            case Some(Seq(j1, j2)) => (j1, j2)
            case _                 => sys.error(s"illegal input, expected [_, _]: ${j.spaces2}")
          }
        }
      )

    def tuple3(that1: JsonShape, that2: JsonShape): JsonShape =
      JsonShape(
        Gen.zip(gen, that1.gen, that2.gen).map {
          case (j1, j2, j3) => Json.fromValues(List(j1, j2, j3))
        },
        Cogen.tuple3(cogen, that1.cogen, that2.cogen).contramap[Json] { j =>
          j.asArray match {
            case Some(Seq(j1, j2, j3)) => (j1, j2, j3)
            case _ =>
              sys.error(s"illegal input, expected: [_, _, _]: ${j.spaces2}")
          }
        }
      )
  }

  def shapes(depth: Int): Gen[JsonShape] = {

    val roots = Gen.oneOf(
      List(
        JsonShape(
          arb[Boolean].map(Json.fromBoolean(_)),
          Cogen[Boolean].contramap[Json] { j =>
            j.asBoolean.getOrElse(sys.error(s"expected bool, got: $j"))
          }
        ),
        JsonShape(
          arb[Long].map(Json.fromLong(_)),
          Cogen[Long].contramap[Json] { j =>
            j.asNumber
              .flatMap(_.toLong)
              .getOrElse(sys.error(s"expected Long, got: $j"))
          }
        ),
        JsonShape(Gen.const(Json.Null), Cogen[Unit].contramap[Json](_ => ())),
        JsonShape(
          arb[String].map(Json.fromString(_)),
          Cogen[String].contramap[Json] { j =>
            j.asString.getOrElse(sys.error(s"expected string got: $j"))
          }
        )
      )
    )

    if (depth <= 0) roots
    else {
      val recur = shapes(depth - 1)
      // we can have tuples, option (null | t), list of some time, records, or maps

      Gen.frequency(
        20 -> roots,
        5 -> recur.map(_.optional),
        2 -> Gen.zip(recur, recur).map { case (s1, s2) => s1.tuple2(s2) },
        1 -> Gen.zip(recur, recur, recur).map {
          case (s1, s2, s3) => s1.tuple3(s2, s3)
        }
      )
    }
  }

  val genShape2: Gen[JsonShape] = shapes(2)

  implicit private val genMonad: Monad[Gen] with Defer[Gen] =
    new Monad[Gen] with Defer[Gen] {
      def defer[A](ga: => Gen[A]): Gen[A] = Gen.lzy(ga)
      def pure[A](a: A): Gen[A] = Gen.const(a)
      override def map[A, B](ga: Gen[A])(fn: A => B): Gen[B] = ga.map(fn)
      def flatMap[A, B](ga: Gen[A])(fn: A => Gen[B]) = ga.flatMap(fn)
      def tailRecM[A, B](a: A)(fn: A => Gen[Either[A, B]]): Gen[B] =
        Gen.tailRecM(a)(fn)
    }

  val genDirectory
      : Gen[(Pua, PuaLocal.Directory, List[Json], Json => IO[Json])] = {
    def genFn(input: Cogen[Json], output: Gen[Json]): Gen[Json => IO[Json]] =
      Gen.function1(output.map(IO.pure(_)))(input)

    type S[A] = StateT[Gen, PuaLocal.Directory, A]
    val genShapeS: S[JsonShape] = StateT.liftF(genShape2)

    def oneOf[A](items: Iterable[S[A]]): S[A] = {
      require(items.nonEmpty, s"can't do oneOf(Nil)")
      val vec = items.toVector
      val sz = vec.size
      val idx = StateT.liftF(Gen.choose(0, sz - 1)): S[Int]

      idx.flatMap(vec(_))
    }

    def fn(
        inshape: Cogen[Json],
        outshape: Gen[Json]
    ): S[(Pua, Json => IO[Json])] = {
      type Fn = Json => IO[Json]

      def kleisli[A, B, C](fn1: A => IO[B], fn2: B => IO[C]): A => IO[C] = {
        a: A => fn1(a).flatMap(fn2)
      }
      def fanout(
          f1: Json => IO[Json],
          f2: Json => IO[Json]
      ): Json => IO[Json] = { in: Json =>
        (f1(in), f2(in)).mapN((o1, o2) => Json.fromValues(List(o1, o2)))
      }

      def parallel(
          f1: Json => IO[Json],
          f2: Json => IO[Json]
      ): Json => IO[Json] = { in: Json =>
        in.asArray match {
          case Some(Seq(i1, i2)) =>
            (f1(i1), f2(i2)).mapN((o1, o2) => Json.fromValues(List(o1, o2)))
          case None =>
            IO.raiseError(new Exception(s"expected array of two things: $in"))
        }
      }

      val genConst: S[(Pua, Fn)] = StateT.liftF(outshape.map { j =>
        (Pua.Const(j), { _: Json => IO.pure(j) })
      })

      lazy val genNewName: S[LambdaFunctionName] =
        for {
          nm <- StateT.liftF(Gen.identifier): S[String]
          dir <- StateT.get: S[PuaLocal.Directory]
          lnm = LambdaFunctionName(nm)
          res <- if (dir.contains(lnm)) genNewName else Monad[S].pure(lnm)
        } yield res

      val call: S[(Pua, Fn)] =
        for {
          nm <- genNewName
          func <- StateT.liftF(genFn(inshape, outshape)): S[Json => IO[Json]]
          _ <- StateT.modify(_.addFn(nm.asString, func)): S[Unit]
        } yield (Pua.Call(nm), func)

      val compose: S[(Pua, Fn)] =
        for {
          middle <- genShapeS
          (p1, f1) <- fn(inshape, middle.gen)
          (p2, f2) <- fn(middle.cogen, outshape)
        } yield (Pua.Compose(p1, p2), kleisli(f1, f2))

      val fanout2: S[(Pua, Fn)] =
        for {
          fn1T <- genShapeS
          fn2T <- genShapeS
          (p1, f1) <- fn(inshape, fn1T.gen)
          (p2, f2) <- fn(inshape, fn2T.gen)
          (p3, f3) <- fn(fn1T.tuple2(fn2T).cogen, outshape)
        } yield (
          Pua.Fanout(NonEmptyList.of(p1, p2)).andThen(p3),
          kleisli(fanout(f1, f2), f3)
        )

      val parallel2: S[(Pua, Fn)] =
        for {
          fn1Ti <- genShapeS
          fn2Ti <- genShapeS
          fn1To <- genShapeS
          fn2To <- genShapeS
          (p0, f0) <- fn(inshape, fn1Ti.tuple2(fn2Ti).gen)
          (p1, f1) <- fn(fn1Ti.cogen, fn1To.gen)
          (p2, f2) <- fn(fn2Ti.cogen, fn2To.gen)
          (p3, f3) <- fn(fn1To.tuple2(fn2To).cogen, outshape)
          resp = p0.andThen(Pua.Parallel(NonEmptyList.of(p1, p2))).andThen(p3)
          resf = kleisli(kleisli(f0, parallel(f1, f2)), f3)
        } yield (resp, resf)

      oneOf(
        List(genConst, call, call, oneOf(List(compose, fanout2, parallel2)))
      )
    }

    val sres = for {
      puaShapeIn <- StateT.liftF(genShape2): S[JsonShape]
      puaShapeOut <- StateT.liftF(genShape2): S[JsonShape]
      (pua, fn) <- fn(puaShapeIn.cogen, puaShapeOut.gen)
      inputs <- StateT.liftF(Gen.listOfN(10, puaShapeIn.gen)): S[List[Json]]
      dir <- StateT.get: S[PuaLocal.Directory]
    } yield (pua, dir, inputs, fn)

    sres.runA(PuaLocal.emptyDirectory)
  }
}
