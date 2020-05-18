package dev.posco.hiona

import cats.{Defer, Monad, Monoid, Semigroup, Semigroupal}
import cats.arrow.FunctionK
import cats.data.{NonEmptyList, StateT, Tuple2K}
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

  implicit def semigroupalTuple2K[F[_]: Semigroupal, G[_]: Semigroupal]
      : Semigroupal[Tuple2K[F, G, *]] =
    new Semigroupal[Tuple2K[F, G, *]] {
      def product[A, B](
          left: Tuple2K[F, G, A],
          right: Tuple2K[F, G, B]
      ): Tuple2K[F, G, (A, B)] =
        Tuple2K(
          Semigroupal[F].product(left.first, right.first),
          Semigroupal[G].product(left.second, right.second)
        )
    }

  implicit def implicitTuple2K[F[_], G[_], A](
      implicit fa: F[A],
      ga: G[A]
  ): Tuple2K[F, G, A] =
    Tuple2K(fa, ga)

  val genTimestamp: Gen[Timestamp] =
    // just before 1970 until just after 2030
    Gen.choose(-1000L, 1900000000000L).map(Timestamp(_))

  implicit val cogenTimestamp: Cogen[Timestamp] =
    Cogen[Long].contramap { ts: Timestamp => ts.epochMillis }

  val genDuration: Gen[Duration] = {
    import Duration._

    for {
      base <- Gen.oneOf(
        List(millisecond, second, minute, hour, day, week, month, quarter, year)
      )
      m <- Gen.choose(0, 10)
    } yield base * m
  }

  // this is a non-commutative monoid, useful for testing
  case class FirstLast[A](first: A, last: A)
  object FirstLast {
    implicit def semigroupFirstLast[A]: Semigroup[FirstLast[A]] =
      new Semigroup[FirstLast[A]] {
        def combine(left: FirstLast[A], right: FirstLast[A]) =
          FirstLast(left.first, right.last)
      }

    implicit def cogenFirstLast[A](implicit cg: Cogen[A]): Cogen[FirstLast[A]] =
      Cogen.tuple2(cg, cg).contramap[FirstLast[A]] {
        case FirstLast(a, b) => (a, b)
      }

    def genFirstLast[A](gen: Gen[A]): Gen[FirstLast[A]] =
      Gen.zip(gen, gen).map { case (a, b) => FirstLast(a, b) }
  }

  /**
    * This is a concat-map that emits <= the
    * same number that it takes in on average
    */
  def genConcatMapFn[A, B](cogen: Cogen[A], gen: Gen[B]): Gen[A => List[B]] = {
    // 1/2 -> 0
    // 1/4 -> 1
    // 1/8 -> 2
    // ...
    // 1/2(0 + 1/2 + .. n/2^n + ...)
    // x = (0 + e^a/2 + ... (e^(an)/2^n) = 1/(1 - e^(a)/2)
    // d/da (x)(a = 0) = 2y
    // 2y = (1/2)/(1 - (1/2))^2 = 4/2 = 2
    // y = 1
    val expSize: Gen[Int] =
      Gen.tailRecM(0) { size =>
        Gen
          .oneOf(false, true)
          .map {
            case false => Right(size)
            case true  => Left(size + 1)
          }
      }

    val bList: Gen[List[B]] =
      expSize.flatMap(Gen.listOfN(_, gen))

    Gen.function1(bList)(cogen)
  }

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

  case class RowResult[A](row: Row[A], result: Result[A]) {
    def zip[B](that: RowResult[B]): RowResult[(A, B)] =
      RowResult(Row.tuple2Row(row, that.row), result.zip(that.result))
  }

  object RowResult {
    implicit def implicitRowResult[A: Row: Result]: RowResult[A] =
      RowResult(implicitly[Row[A]], implicitly[Result[A]])

    implicit def semigroupAlRowResult: Semigroupal[RowResult] =
      new Semigroupal[RowResult] {
        def product[A, B](fa: RowResult[A], fb: RowResult[B]) = fa.zip(fb)
      }

    val toResult: FunctionK[RowResult, Result] =
      new FunctionK[RowResult, Result] {
        def apply[A](src: RowResult[A]): Result[A] = src.result
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

  import Simulator.InputData

  private def distinctBy[A, B](as: List[A])(fn: A => B): List[A] =
    as.foldLeft((Set.empty[B], List.empty[A])) {
        case (acc @ (bs, rev), a) =>
          val b = fn(a)
          if (bs(b)) acc
          else (bs + b, a :: rev)
      }
      ._2
      .reverse

  case class ResultSrc[A](result: Result[A], src: Event.Source[A]) {
    // generate input timestamps with no collisions
    // collisions give very unclear semantics with regards
    // to before and after
    def genInputData(size: Int): Gen[InputData[A]] = {
      def genItems(
          size: Int,
          maxTrials: Int
      ): Gen[List[(A, Timestamp)]] =
        Gen.tailRecM((size, maxTrials, List.empty[(A, Timestamp)])) {
          case (size, maxTrials, tail) =>
            if (maxTrials <= 0) Gen.const(Right(tail))
            else {
              Gen
                .listOfN(size, result.gen)
                .map { as =>
                  val valids = as
                    .map(a => src.validator.validate(a).map((a, _)))
                    .collect { case Right(good) => good }

                  val nextTail = valids reverse_::: tail
                  val missing = size - valids.size
                  if (missing == 0) Right(nextTail)
                  else {
                    Left((missing, maxTrials - 1, nextTail))
                  }
                }
            }
        }

      genItems(size, size)
        .map { data0 =>
          val data = distinctBy(data0)(_._2)
          val sorted = data.sortBy(_._2)
          InputData(sorted.map(_._1), src)
        }
    }
  }

  case class History(
      names: Set[String],
      sources: Set[TypeWith[ResultSrc]],
      events: Set[TypeWith[ResultEv]]
  ) {

    def hasName(nm: String): Boolean = names(nm)
  }

  object History {
    val empty: History = History(Set.empty, Set.empty, Set.empty)
  }

  /**
    * we use the state to keep a set of previously
    * generated Events and types
    */
  type GenS[A] = StateT[Gen, History, A]

  /**
    * Pick one of these
    */
  def frequencyS[A](nel: NonEmptyList[(Int, GenS[A])]): GenS[A] = {
    nel.toList.foreach {
      case (w, _) => require(w >= 0, s"weights must be >= 0, found $w in $nel")
    }

    val totalWeight = nel.foldMap { case (w, _) => w.toLong }
    require(
      totalWeight >= 1L,
      s"we need a weight of at least 1, got: $totalWeight"
    )

    lift(Gen.choose(0L, totalWeight - 1L))
      .flatMap { idx =>
        @annotation.tailrec
        def loop(idx: Long, items: NonEmptyList[(Int, GenS[A])]): GenS[A] =
          items match {
            case NonEmptyList((_, last), Nil) => last
            case NonEmptyList((w, h0), head :: tail) =>
              if (idx <= w.toLong) h0
              else loop(idx - w.toLong, NonEmptyList(head, tail))
          }

        loop(idx, nel)
      }
  }

  def lift[A](gen: Gen[A]): GenS[A] =
    StateT.liftF(gen)

  val primTypes: List[TypeWith[RowResult]] =
    List(
      TypeWith[RowResult, Boolean],
      TypeWith[RowResult, Byte],
      TypeWith[RowResult, Char],
      TypeWith[RowResult, Int],
      TypeWith[RowResult, Long],
      TypeWith[RowResult, String]
    )

  val genSrcType: Gen[TypeWith[RowResult]] =
    Defer[Gen].fix[TypeWith[RowResult]] { recur =>
      Gen.frequency(
        2 * primTypes.size -> Gen.oneOf(primTypes),
        1 -> Gen.zip(recur, recur).map { case (a, b) => a.product(b) }
      )
    }

  val genType: Gen[TypeWith[Result]] =
    // size = (1-p) * s0 + p * 2 * size
    // size = (1-p) * s0 / (1 - 2*p)
    // so we need p << 1/2 to avoid diverging
    Defer[Gen].fix[TypeWith[Result]] { recur =>
      Gen.frequency(
        10 -> genSrcType.map(_.mapK(RowResult.toResult)),
        1 -> Gen.zip(recur, recur).map { case (a, b) => a.product(b) }
      )
    }

  val genMonoidType: Gen[TypeWith[Tuple2K[Result, Monoid, *]]] =
    Defer[Gen].fix[TypeWith[Tuple2K[Result, Monoid, *]]] { recur =>
      val prim = List(
        TypeWith[Tuple2K[Result, Monoid, *], Unit],
        TypeWith[Tuple2K[Result, Monoid, *], Int],
        TypeWith[Tuple2K[Result, Monoid, *], Long]
      )

      // this is a non-commutative monoid
      val fl =
        for {
          res <- genType
          resFL: Result[Option[FirstLast[res.Type]]] = Result(
            Cogen.cogenOption(FirstLast.cogenFirstLast(res.evidence.cogen)),
            Gen.option(FirstLast.genFirstLast(res.evidence.gen))
          )
          mon: Monoid[Option[FirstLast[res.Type]]] = Monoid[Option[
            FirstLast[res.Type]
          ]]
          tup = Tuple2K(resFL, mon)
        } yield TypeWith(tup): TypeWith[Tuple2K[Result, Monoid, *]]

      val pair = Gen.lzy(for {
        a <- recur
        b <- recur
      } yield a.product(b))

      Gen.frequency(5 -> Gen.oneOf(prim), 1 -> fl, 1 -> pair)
    }

  def genEvent(t: TypeWith[Result]): GenS[Event[t.Type]] = {
    val src =
      for {
        stype <- lift(genSrcType)
        srcEvent <- genSource(Gen.identifier, stype)
        mapToCorrect: Event[t.Type] <- genMap(
          stype.mapK(RowResult.toResult),
          t
        )(srcEvent)
      } yield mapToCorrect

    val map =
      for {
        firstT <- lift(genType)
        ev1 <- genEvent(firstT)
        ev2 <- genMap(firstT, t)(ev1)
      } yield ev2

    val concatMap =
      for {
        firstT <- lift(genType)
        ev1 <- genEvent(firstT)
        ev2 <- genConcatMapped(firstT, t)(ev1)
      } yield ev2

    val concat = Defer[GenS].defer(genConcat(t))

    val kv =
      for {
        k <- lift(genType)
        v <- lift(genType)
        ev <- genEvent2(k, v)
        evt <- genMap(k.product(v), t)(ev)
      } yield evt

    frequencyS(
      NonEmptyList.of(
        1 -> Monad[GenS].pure(Event.Empty),
        11 -> src,
        4 -> map,
        4 -> Defer[GenS].defer(genFilter(t)),
        4 -> Defer[GenS].defer(genWithTime(t)),
        2 -> concatMap,
        1 -> kv,
        1 -> concat // this can cause the graphs to get huge, this probability needs to be small
      )
    )
  }

  def genFeature(
      k: TypeWith[Result],
      v: TypeWith[Result]
  ): GenS[Feature[k.Type, v.Type]] = {
    val genSum: GenS[Feature[k.Type, v.Type]] =
      for {
        v0 <- lift(genMonoidType)
        monV0: Monoid[v0.Type] = v0.evidence.second
        resV0: Result[v0.Type] = v0.evidence.first
        ev <- genEvent2(k, TypeWith(resV0))
        fn: Function1[v0.Type, v.Type] <- lift(
          Gen.function1(v.evidence.gen)(resV0.cogen)
        )
      } yield ev.sum(monV0).map(fn)

    def cogenOpt[A](implicit cg: Cogen[A]): Cogen[Option[A]] =
      implicitly

    val genLatest: GenS[Feature[k.Type, v.Type]] =
      for {
        v0 <- lift(genType)
        dur <- lift(genDuration)
        ev <- genEvent2(k, v0)
        fn: Function1[Option[v0.Type], v.Type] <- lift(
          Gen.function1(v.evidence.gen)(cogenOpt(v0.evidence.cogen))
        )
      } yield ev.latest(dur).map(fn)

    val genZip: GenS[Feature[k.Type, v.Type]] =
      for {
        v0 <- lift(genType)
        v1 <- lift(genType)
        f1 <- genFeature(k, v0)
        f2 <- genFeature(k, v1)
        fn: Function1[(v0.Type, v1.Type), v.Type] <- lift(
          Gen.function1(v.evidence.gen)(v0.product(v1).evidence.cogen)
        )
      } yield f1.zip(f2).map(fn)

    frequencyS(
      NonEmptyList.of(
        5 -> genSum,
        5 -> genLatest,
        1 -> genZip
      )
    )
  }

  def genEvent2(
      k: TypeWith[Result],
      v: TypeWith[Result]
  ): GenS[Event[(k.Type, v.Type)]] = {
    // it could just be a regular pair
    val pair = Defer[GenS].defer(genEvent(k.product(v)))

    val genLookup =
      for {
        v0 <- lift(genType)
        v1 <- lift(genType)
        ev <- genEvent2(k, v0)
        feat <- genFeature(k, v1)
        pre = ev.preLookup(feat)
        post = ev.postLookup(feat)
        ev1 <- lift(Gen.oneOf(pre, post))
        fn: Function1[(v0.Type, v1.Type), v.Type] <- lift(
          Gen.function1(v.evidence.gen)(v0.product(v1).evidence.cogen)
        )
        ev2 = ev1.mapValues(fn)
        _ <- add(k.product(v).evidence, ev2)
      } yield ev2

    val mapValues =
      for {
        v0 <- lift(genType)
        ev <- genEvent2(k, v0)
        fn: Function1[v0.Type, v.Type] <- lift(
          Gen.function1(v.evidence.gen)(v0.evidence.cogen)
        )
        ev2 = ev.mapValues(fn)
        _ <- add(k.product(v).evidence, ev2)
      } yield ev2

    frequencyS(
      NonEmptyList.of(
        4 -> pair,
        3 -> genLookup,
        1 -> mapValues
      )
    )
  }

  val genSEventTW: GenS[TypeWith[ResultEv]] =
    for {
      twr <- lift(genType)
      ev <- genEvent(twr)
    } yield TypeWith[ResultEv, twr.Type](ResultEv(twr.evidence, ev))

  val genSrcsAndEventTW: Gen[(Set[TypeWith[ResultSrc]], TypeWith[ResultEv])] =
    genSEventTW
      .run(History.empty)
      .map {
        case (hist, tw) =>
          (hist.sources, tw)
      }

  def genWithSizedSrc[A](
      gsize: Gen[Int],
      ga: GenS[A]
  ): Gen[(List[TypeWith[InputData]], A)] =
    ga.run(History.empty)
      .flatMap {
        case (ressrc, a) =>
          ressrc.sources.toList
            .sortBy(_.evidence.src.name)
            .traverse { twr =>
              // we need to ascribe the type
              val res: Gen[TypeWith[InputData]] =
                twr.mapK2(
                  new FunctionK[ResultSrc, Lambda[x => Gen[InputData[x]]]] {
                    def apply[Z](ra: ResultSrc[Z]) =
                      gsize.flatMap(ra.genInputData(_))
                  }
                )
              res
            }
            .map((_, a))
      }

  def genSrcsSizedAndEvent(
      gsize: Gen[Int]
  ): Gen[(List[TypeWith[InputData]], TypeWith[ResultEv])] =
    genWithSizedSrc(gsize, genSEventTW)

  val genEventTW: Gen[TypeWith[Event]] =
    genSrcsAndEventTW.map(_._2.mapK(ResultEv.toEvent))

  def genDefault(targetSize: Int): Gen[(Simulator.Inputs, TypeWith[Event])] =
    GenEventFeature
      .genSrcsSizedAndEvent(Gen.const(targetSize))
      .map {
        case (i, twev) => (i, twev.mapK(GenEventFeature.ResultEv.toEvent))
      }

  def add[A](res: Result[A], ev: Event[A]): GenS[Unit] =
    StateT.modify(h =>
      h.copy(events = h.events + TypeWith[ResultEv, A](ResultEv(res, ev)))
    )

  def addSource[A](res: Result[A], ev: Event.Source[A]): GenS[Unit] =
    StateT.modify { history =>
      History(
        history.names + ev.name,
        history.sources + TypeWith(ResultSrc(res, ev)),
        history.events + TypeWith(ResultEv(res, ev))
      )
    }

  def genNewName(genName: Gen[String]): GenS[String] = {
    val gensName = lift(genName)

    gensName.flatMap { name0 =>
      Monad[GenS].tailRecM(name0) { candidate =>
        val used: GenS[History] = StateT.get

        used.flatMap { hist =>
          if (hist.hasName(candidate)) gensName.map(Left(_))
          else Monad[GenS].pure(Right(candidate))
        }
      }
    }
  }

  def validatorFor(t: TypeWith[RowResult]): Gen[Validator[t.Type]] = {
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
      t1: TypeWith[RowResult]
  ): GenS[Event.Source[t1.Type]] =
    for {
      name <- genNewName(genName)
      validator <- lift(validatorFor(t1))
      src: Event.Source[t1.Type] = Event.source[t1.Type](name, validator)(
        t1.evidence.row
      )
      _ <- addSource[t1.Type](t1.evidence.result, src)
    } yield src

  def genMap(t0: TypeWith[Result], t1: TypeWith[Result])(
      ev0: Event[t0.Type]
  ): GenS[Event[t1.Type]] =
    for {
      fn: (t0.Type => t1.Type) <- lift(
        Gen.function1(t1.evidence.gen)(t0.evidence.cogen)
      )
      ev1: Event[t1.Type] = ev0.map(fn)
      _ <- add(t1.evidence, ev1)
    } yield ev1

  def genConcatMapped(t0: TypeWith[Result], t1: TypeWith[Result])(
      ev0: Event[t0.Type]
  ): GenS[Event[t1.Type]] =
    for {
      fn: (t0.Type => List[t1.Type]) <- lift(
        genConcatMapFn(t0.evidence.cogen, t1.evidence.gen)
      )
      ev1: Event[t1.Type] = ev0.concatMap(fn)
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
      ev1: Event[(tw.Type, Timestamp)] = ev0.withTime
      result: Result[(tw.Type, Timestamp)] = twr.evidence.zip(
        Result(Cogen[Timestamp], genTimestamp)
      )
      _ <- add(result, ev1)
      ev2 <- genMap(TypeWith(result), t1)(ev1)
    } yield ev2

  // return a previous event mapped onto the current type
  // if there has been no previous event, call genEvent
  def genPrevious(t1: TypeWith[Result]): GenS[Event[t1.Type]] = {
    val used: GenS[History] = StateT.get

    used.flatMap { hist =>
      NonEmptyList.fromList(hist.events.toList) match {
        case None => genEvent(t1)
        case Some(nel) =>
          frequencyS(nel.map(twre => (1, Monad[GenS].pure(twre))))
            .flatMap { twre =>
              genMap(twre.mapK(ResultEv.toResult), t1)(twre.evidence.event)
            }
      }
    }
  }

  def genPair(t1: TypeWith[Result]): GenS[(Event[t1.Type], Event[t1.Type])] =
    for {
      e1 <- genEvent(t1)
      nonTree = Defer[GenS].defer(genPrevious(t1))
      treeLike = Defer[GenS].defer(genEvent(t1))
      selfConcat = Monad[GenS].pure(e1)
      e2 <- frequencyS(
        NonEmptyList.of(4 -> treeLike, 1 -> nonTree, 1 -> selfConcat)
      )
    } yield (e1, e2)

  def genConcat(t1: TypeWith[Result]): GenS[Event[t1.Type]] =
    for {
      (e1, e2) <- genPair(t1)
      e3 = e1 ++ e2
      _ <- add(t1.evidence, e3)
    } yield e3
}
