package dev.posco.hiona

import cats.{Defer, Monad, Semigroupal}
import cats.arrow.FunctionK
import cats.data.{NonEmptyList, StateT}
import org.scalacheck.{Arbitrary, Cogen, Gen}

import cats.implicits._

object GenEventFeature {
  implicit val genMonad: Monad[Gen] with Defer[Gen] =
    new Monad[Gen] with Defer[Gen] {
      def defer[A](ga: => Gen[A]): Gen[A] = Gen.lzy(ga)
      def pure[A](a: A): Gen[A] = Gen.const(a)
      override def map[A, B](ga: Gen[A])(fn: A => B): Gen[B] = ga.map(fn)
      def flatMap[A, B](ga: Gen[A])(fn: A => Gen[B]) = ga.flatMap(fn)
      def tailRecM[A, B](a: A)(fn: A => Gen[Either[A, B]]): Gen[B] =
        Gen.tailRecM(a)(fn)
    }

  val genTimestamp: Gen[Timestamp] =
    // just before 1970 until just after 2030
    Gen.choose(-1000L, 1900000000000L).map(Timestamp(_))

  implicit val cogenTimestamp: Cogen[Timestamp] =
    Cogen[Long].contramap { ts: Timestamp => ts.epochMillis }

  case class Result[A](cogen: Cogen[A], gen: Gen[A]) {
    def zip[B](that: Result[B]): Result[(A, B)] =
      Result(Cogen.tuple2(cogen, that.cogen), Gen.zip(gen, that.gen))
  }

  object Result {
    implicit def implicitResult[A: Cogen: Arbitrary]: Result[A] =
      Result(Cogen[A], Arbitrary.arbitrary[A])

    implicit def semigroupAlResult: Semigroupal[Result] =
      new Semigroupal[Result] {
        def product[A, B](fa: Result[A], fb: Result[B]) = fa.zip(fb)
      }
  }

  case class SrcResult[A](row: Row[A], result: Result[A]) {
    def zip[B](that: SrcResult[B]): SrcResult[(A, B)] =
      SrcResult(Row.tuple2Row(row, that.row), result.zip(that.result))
  }

  object SrcResult {
    implicit def implicitSrcResult[A: Row: Result]: SrcResult[A] =
      SrcResult(implicitly[Row[A]], implicitly[Result[A]])

    implicit def semigroupAlSrcResult: Semigroupal[SrcResult] =
      new Semigroupal[SrcResult] {
        def product[A, B](fa: SrcResult[A], fb: SrcResult[B]) = fa.zip(fb)
      }

    val toResult: FunctionK[SrcResult, Result] =
      new FunctionK[SrcResult, Result] {
        def apply[A](src: SrcResult[A]): Result[A] = src.result
      }
  }

  case class ResultEv[A](result: Result[A], event: Event[A])

  object ResultEv {
    val toResult: FunctionK[ResultEv, Result] =
      new FunctionK[ResultEv, Result] {
        def apply[A](src: ResultEv[A]): Result[A] = src.result
      }

    val toEvent: FunctionK[ResultEv, Event] =
      new FunctionK[ResultEv, Event] {
        def apply[A](src: ResultEv[A]): Event[A] = src.event
      }
  }

  /**
    * we use the state to keep a set of previously
    * generated Events and types
    */
  type GenS[A] = StateT[Gen, Set[TypeWith[ResultEv]], A]

  /**
    * Pick one of these
    */
  def frequencyS[A](nel: NonEmptyList[(Int, GenS[A])]): GenS[A] = {
    nel.toList.foreach {
      case (w, _) => require(w > 0, s"weights must be > 0, found $w in $nel")
    }

    val totalWeight = nel.foldMap { case (w, _) => w.toLong }

    lift(Gen.choose(0L, totalWeight - 1L))
      .flatMap { idx =>
        @annotation.tailrec
        def loop(idx: Long, items: NonEmptyList[(Int, GenS[A])]): GenS[A] =
          items match {
            case NonEmptyList((_, last), Nil) => last
            case NonEmptyList((w, h0), head :: tail) =>
              if (idx <= 0) h0
              else loop(idx - w.toLong, NonEmptyList(head, tail))
          }

        loop(idx, nel)
      }
  }

  def lift[A](gen: Gen[A]): GenS[A] =
    StateT.liftF(gen)

  def runS[A](gens: GenS[A]): Gen[A] =
    gens.run(Set.empty).map(_._2)

  val primTypes: List[TypeWith[SrcResult]] =
    List(
      TypeWith[SrcResult, Byte],
      TypeWith[SrcResult, Char],
      TypeWith[SrcResult, Int],
      TypeWith[SrcResult, Long],
      TypeWith[SrcResult, String]
    )

  val genSrcType: Gen[TypeWith[SrcResult]] =
    Defer[Gen].fix[TypeWith[SrcResult]] { recur =>
      Gen.frequency(
        primTypes.size -> Gen.oneOf(primTypes),
        1 -> Gen.zip(recur, recur).map { case (a, b) => a.product(b) }
      )
    }

  val genType: Gen[TypeWith[Result]] =
    Defer[Gen].fix[TypeWith[Result]] { recur =>
      Gen.frequency(
        10 -> genSrcType.map(_.mapK(SrcResult.toResult)),
        1 -> Gen.zip(recur, recur).map { case (a, b) => a.product(b) }
      )
    }

  def genEvent(t: TypeWith[Result]): GenS[Event[t.Type]] = {
    val src =
      for {
        stype <- lift(genSrcType)
        srcEvent <- genSource(Gen.identifier, stype)
        mapToCorrect <- genMap(stype.mapK(SrcResult.toResult), t)(srcEvent)
      } yield mapToCorrect

    val map =
      for {
        firstT <- lift(genType)
        ev1 <- genEvent(firstT)
        ev2 <- genMap(firstT, t)(ev1)
      } yield ev2

    val concat = Defer[GenS].defer(genConcat(t))

    frequencyS(
      NonEmptyList.of(
        10 -> src,
        4 -> map,
        1 -> Defer[GenS].defer(genFilter(t)),
        1 -> Defer[GenS].defer(genWithTime(t)),
        1 -> concat // this can cause the graphs to get huge, this probability needs to be small
      )
    )
  }

  val genSEventTW: GenS[TypeWith[ResultEv]] =
    for {
      twr <- lift(genType)
      ev <- genEvent(twr)
    } yield TypeWith[ResultEv, twr.Type](ResultEv(twr.evidence, ev))

  val genEventTW: Gen[TypeWith[Event]] =
    runS(genSEventTW.map(_.mapK(ResultEv.toEvent)))

  def add[A](res: Result[A], ev: Event[A]): GenS[Unit] =
    StateT.modify(s => s + TypeWith[ResultEv, A](ResultEv(res, ev)))

  def genNewName(genName: Gen[String]): GenS[String] = {
    val gensName = lift(genName)

    gensName.flatMap { name0 =>
      Monad[GenS].tailRecM(name0) { candidate =>
        val used: GenS[Set[TypeWith[ResultEv]]] = StateT.get

        used.flatMap { set =>
          val names = set.iterator
            .map(_.evidence)
            .collect {
              case ResultEv(_, Event.Source(name, _, _)) => name
            }
            .toSet

          if (names(candidate)) gensName.map(Left(_))
          else Monad[GenS].pure(Right(candidate))
        }
      }
    }
  }

  def validatorFor(t: TypeWith[SrcResult]): Gen[Validator[t.Type]] = {
    val gfn: Gen[t.Type => Option[Timestamp]] =
      Gen.function1(Gen.option(genTimestamp))(t.evidence.result.cogen)

    gfn.map { fn =>
      new Validator[t.Type] {
        def validate(tt: t.Type) =
          fn(tt) match {
            case Some(ts) => Right(ts)
            case None     => Left(Validator.MissingTimestamp(tt))
          }
      }
    }
  }

  def genSource(
      genName: Gen[String],
      t1: TypeWith[SrcResult]
  ): GenS[Event.Source[t1.Type]] =
    for {
      name <- genNewName(genName)
      validator <- lift(validatorFor(t1))
      src: Event.Source[t1.Type] = Event.source[t1.Type](name, validator)(
        t1.evidence.row
      )
      _ <- add[t1.Type](t1.evidence.result, src)
    } yield src

  def genMap(t0: TypeWith[Result], t1: TypeWith[Result])(
      ev0: Event[t0.Type]
  ): GenS[Event[t1.Type]] =
    for {
      fn <- lift(Gen.function1(t1.evidence.gen)(t0.evidence.cogen))
      ev1 = ev0.map(fn)
      _ <- add(t1.evidence, ev1)
    } yield ev1

  def genFilter(t1: TypeWith[Result]): GenS[Event[t1.Type]] =
    for {
      ev0 <- genEvent(t1)
      fn <- lift(Gen.function1(Gen.oneOf(true, false))(t1.evidence.cogen))
      ev1 = ev0.filter(fn)
      _ <- add(t1.evidence, ev1)
    } yield ev1

  def genWithTime(t1: TypeWith[Result]): GenS[Event[t1.Type]] =
    for {
      tw: TypeWith[ResultEv] <- genSEventTW
      twr: TypeWith.Aux[Result, tw.Type] = tw.mapK(ResultEv.toResult)
      ev0: Event[tw.Type] = tw.evidence.event
      ev1 = ev0.withTime
      result = twr.evidence.zip(Result(Cogen[Timestamp], genTimestamp))
      _ <- add(result, ev1)
      ev2 <- genMap(TypeWith(result), t1)(ev1)
    } yield ev2

  // return a previous event mapped onto the current type
  // if there has been no previous event, call genEvent
  def genPrevious(t1: TypeWith[Result]): GenS[Event[t1.Type]] = {
    val used: GenS[Set[TypeWith[ResultEv]]] = StateT.get

    used.flatMap { prev =>
      NonEmptyList.fromList(prev.toList) match {
        case None => genEvent(t1)
        case Some(nel) =>
          frequencyS(nel.map(twre => (1, Monad[GenS].pure(twre))))
            .flatMap { twre =>
              genMap(twre.mapK(ResultEv.toResult), t1)(twre.evidence.event)
            }
      }
    }
  }

  def genConcat(t1: TypeWith[Result]): GenS[Event[t1.Type]] =
    for {
      e1 <- genEvent(t1)
      nonTree = Defer[GenS].defer(genPrevious(t1))
      treeLike = Defer[GenS].defer(genEvent(t1))
      selfConcat = Monad[GenS].pure(e1)
      e2 <- frequencyS(
        NonEmptyList.of(4 -> treeLike, 1 -> nonTree, 1 -> selfConcat)
      )
      e3 = e1 ++ e2
      _ <- add(t1.evidence, e3)
    } yield e3
}
