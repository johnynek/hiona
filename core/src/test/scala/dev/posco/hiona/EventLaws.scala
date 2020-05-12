package dev.posco.hiona

import cats.Monoid
import org.scalacheck.{Gen, Prop}

class EventLaws extends munit.ScalaCheckSuite {
  //override val scalaCheckInitialSeed = "A3thV6W0_1wDjBOi4dHhs9mmpZf9VCYS6xol9HUXbDM="
  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        100
      ) // a bit slow, but locally, this passes with 10000

  property("Simulator.merge is correct") {
    Prop.forAll { (l1: List[Int], l2: List[Int]) =>
      val l3 = (l1 reverse_::: l2).sorted
      val l3m =
        Simulator.mergeIterators(l1.sorted.iterator, l2.sorted.iterator).toList
      val l3ll =
        Simulator.merge(l1.sorted.to(LazyList), l2.sorted.to(LazyList)).toList

      assertEquals(l3m, l3)
      assertEquals(l3ll, l3)
    }
  }

  property("Event.triggersOf has the same sources") {
    Prop.forAll(GenEventFeature.genEventTW) { tw =>
      val ev: Event[tw.Type] = tw.evidence

      val triggers = Event.triggersOf(ev.map((_, ())))

      val s1 = Event.sourcesOf(ev)
      val s2 = Event.sourcesOf(triggers)

      assertEquals(s2, s1)

      Prop(s1 == s2)
    }
  }

  property("Event.map Simulator works") {
    val genFn
        : Gen[(Simulator.Inputs, TypeWith[Lambda[x => (Event[x], x => Int)]])] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .flatMap {
          case (inputs, twresev) =>
            val fnGen: Gen[twresev.Type => Int] =
              Gen.function1(Gen.choose(-100, 100))(
                twresev.evidence.result.cogen
              )

            fnGen.map { fn =>
              (
                inputs,
                TypeWith[Lambda[x => (Event[x], x => Int)], twresev.Type](
                  (twresev.evidence.event, fn)
                )
              )
            }
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence._1
        val fn: twev.Type => Int = twev.evidence._2

        val mapped =
          Simulator.eventToLazyList(ev.map(fn), inputs)

        val unmapped =
          Simulator
            .eventToLazyList(ev, inputs)
            .map { case (p, x) => (p, fn(x)) }

        assertEquals(mapped, unmapped)
    }
  }

  property("Event.concatMap Simulator works") {
    val genFn: Gen[
      (Simulator.Inputs, TypeWith[Lambda[x => (Event[x], x => List[Int])]])
    ] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .flatMap {
          case (inputs, twresev) =>
            val fnGen: Gen[twresev.Type => List[Int]] =
              GenEventFeature.genConcatMapFn(
                twresev.evidence.result.cogen,
                Gen.choose(-100, 100)
              )

            fnGen.map { fn =>
              (
                inputs,
                TypeWith[Lambda[x => (Event[x], x => List[Int])], twresev.Type](
                  (twresev.evidence.event, fn)
                )
              )
            }
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence._1
        val fn: twev.Type => List[Int] = twev.evidence._2

        val mapped =
          Simulator.eventToLazyList(ev.concatMap(fn), inputs)

        val unmapped =
          Simulator
            .eventToLazyList(ev, inputs)
            .flatMap { case (p, xs) => fn(xs).map((p, _)) }

        assertEquals(mapped, unmapped)
    }
  }

  property("Event.filter Simulator works") {
    val genFn: Gen[
      (Simulator.Inputs, TypeWith[Lambda[x => (Event[x], x => Boolean)]])
    ] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .flatMap {
          case (inputs, twresev) =>
            val fnGen: Gen[twresev.Type => Boolean] =
              Gen.function1(Gen.oneOf(false, true))(
                twresev.evidence.result.cogen
              )

            fnGen.map { fn =>
              (
                inputs,
                TypeWith[Lambda[x => (Event[x], x => Boolean)], twresev.Type](
                  (twresev.evidence.event, fn)
                )
              )
            }
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence._1
        val fn: twev.Type => Boolean = twev.evidence._2

        val unmapped =
          Simulator
            .eventToLazyList(ev, inputs)
            .filter { case (_, x) => fn(x) }
            .toList

        val mapped =
          Simulator.eventToLazyList(ev.filter(fn), inputs).toList

        assertEquals(mapped, unmapped)

        Prop(mapped == unmapped)
    }
  }

  property("Event.withTime Simulator works") {

    val genFn: Gen[(Simulator.Inputs, TypeWith[Event])] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .map {
          case (i, twev) => (i, twev.mapK(GenEventFeature.ResultEv.toEvent))
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence

        val unmapped =
          Simulator
            .eventToLazyList(ev, inputs)
            .map { case (p, x) => (p, (x, p.ts)) }

        val mapped =
          Simulator.eventToLazyList(ev.withTime, inputs)

        assertEquals(mapped, unmapped)

        Prop(mapped == unmapped)
    }
  }

  def assertUnorderedEq[A](left: List[A], right: List[A]): Prop = {
    val leftMap = left.groupBy(identity).map { case (k, v)   => (k, v.size) }
    val rightMap = right.groupBy(identity).map { case (k, v) => (k, v.size) }

    assertEquals(leftMap, rightMap)

    Prop(leftMap == rightMap)
  }

  property("(x ++ x) == x.concatMap { y => List(y, y) } for Simulator") {

    val genFn: Gen[(Simulator.Inputs, TypeWith[Event])] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .map {
          case (i, twev) => (i, twev.mapK(GenEventFeature.ResultEv.toEvent))
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence

        val unmapped =
          Simulator.eventToLazyList(ev ++ ev, inputs)

        val mapped =
          Simulator.eventToLazyList(ev.concatMap(y => List(y, y)), inputs)

        assertUnorderedEq(mapped.toList, unmapped.toList)
    }
  }

  property("(x ++ y) is the same as Simulator(x) merge Simulator(y)") {
    type Pair[T] = (Event[T], Event[T])
    import GenEventFeature.{genMonad, Result}

    val genFn: Gen[(Simulator.Inputs, TypeWith[Pair])] =
      GenEventFeature
        .genWithSizedSrc(
          Gen.const(100),
          for {
            tpe: TypeWith[Result] <- GenEventFeature.lift(
              GenEventFeature.genType
            )
            pair <- GenEventFeature.genPair(tpe)
            tw: TypeWith[Pair] = TypeWith[Pair, tpe.Type](pair)
          } yield tw
        )

    Prop.forAllNoShrink(genFn) {
      case (inputs, twpair) =>
        val e1: Event[twpair.Type] = twpair.evidence._1
        val e2: Event[twpair.Type] = twpair.evidence._2

        val v1 =
          Simulator.eventToLazyList(e1 ++ e2, inputs)

        val v2a = Simulator.eventToLazyList(e1, inputs)
        val v2b = Simulator.eventToLazyList(e2, inputs)

        val v2 = Simulator.merge(v2a, v2b)(Ordering[Point].on(_._1))
        assertEquals(v1, v2)

        Prop(v1 == v2)
    }

  }

  property("we are sorted by timestamp at the end") {
    val genFn: Gen[(Simulator.Inputs, TypeWith[Event])] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .map {
          case (i, twev) => (i, twev.mapK(GenEventFeature.ResultEv.toEvent))
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence

        val res = Simulator.eventToLazyList(ev, inputs).map(_._1.ts).toList
        val sortRes = res.sorted
        assertEquals(res, sortRes)

        Prop(res == sortRes)
    }
  }

  property("amplification is accurate") {
    val genFn: Gen[(Simulator.Inputs, TypeWith[Event])] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(1)) // put a single event
        .map {
          case (i, twev) => (i, twev.mapK(GenEventFeature.ResultEv.toEvent))
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        if ((inputs.size == 1) && (inputs.head.evidence.inputs.nonEmpty)) {
          // only one source
          val ev: Event[twev.Type] = twev.evidence
          val amp = Event.amplificationOf(ev)

          val res = Simulator.eventToLazyList(ev, inputs).size

          assert(amp(res), s"$amp: $res")
          Prop(amp(res))

        } else Prop(true)
    }
  }

  property("kvs.postLookup(kvs.latest)... should be identity Simulator") {

    import GenEventFeature.{genEvent2, genMonad, genType, genWithSizedSrc, lift}

    val genFn: Gen[(Simulator.Inputs, TypeWith2[Event.Keyed])] =
      genWithSizedSrc(Gen.const(100), for {
        k <- lift(genType)
        v <- lift(genType)
        ev <- genEvent2(k, v)
        tw: TypeWith2[Event.Keyed] = TypeWith2[Event.Keyed, k.Type, v.Type](ev)
      } yield tw)

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[(twev.Type1, twev.Type2)] = twev.evidence
        val amp = Event.amplificationOf(ev)

        if (amp.atMostOne) {
          val ev1 = ev
            .postLookup(ev.latest(Duration.Infinite))
            .map {
              case (k, (v, None)) =>
                sys.error(s"expected after to find a value: $k $v")
              case (k, (v, Some(w))) =>
                assertEquals(w, v)
                (k, v)
            }

          val kres =
            Simulator.eventToLazyList(ev, inputs).toList

          val kres1 =
            Simulator.eventToLazyList(ev1, inputs).toList

          assertEquals(kres1, kres)
          Prop(kres == kres1)
        } else Prop(true)
    }
  }

  property("kvs.postLookup(kvs.sum)... should be an in order sum Simulator") {

    import GenEventFeature.{
      genEvent2,
      genMonad,
      genMonoidType,
      genType,
      genWithSizedSrc,
      lift,
      Result
    }

    type KV[K, V] = (Event[(K, V)], Monoid[V])

    val genFn: Gen[(Simulator.Inputs, TypeWith2[KV])] =
      genWithSizedSrc(
        Gen.const(100),
        for {
          k <- lift(genType)
          vrm <- lift(genMonoidType)
          v = TypeWith[Result, vrm.Type](vrm.evidence.first)
          ev <- genEvent2(k, v)
          tw: TypeWith2[KV] = TypeWith2[KV, k.Type, v.Type](
            (ev, vrm.evidence.second)
          )
        } yield tw
      )

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val (ev, monoid): (Event[(twev.Type1, twev.Type2)], Monoid[twev.Type2]) =
          twev.evidence

        val atMostOne = Event.amplificationOf(ev).atMostOne

        if (atMostOne) {
          val ev1 = ev
            .postLookup(ev.sum(monoid))
            .mapValues(_._2)

          def summed[T, K, V](
              kvs: LazyList[(T, (K, V))],
              acc: Map[K, V],
              monoid: Monoid[V]
          ): LazyList[(T, (K, V))] =
            kvs match {
              case (t, (k, v)) #:: tail =>
                val vsum = acc.get(k) match {
                  case Some(v0) => monoid.combine(v0, v)
                  case None     => v
                }
                val acc1 = acc.updated(k, vsum)
                (t, (k, vsum)) #:: summed(tail, acc1, monoid)
              case LazyList() => LazyList()
            }

          val list0 = Simulator.eventToLazyList(ev, inputs)
          val list1 = Simulator.eventToLazyList(ev1, inputs)

          val summed0 = summed(list0, Map.empty, monoid)

          assertEquals(list1, summed0)
          Prop(list1 == summed0)
        } else Prop(true)
    }
  }

  property(
    "x.asKeys.preLookup(x.asKeys.latest) can be used to dedup for Simulator"
  ) {

    val genFn: Gen[(Simulator.Inputs, TypeWith[Event])] =
      GenEventFeature
        .genSrcsSizedAndEvent(Gen.const(100))
        .map {
          case (i, twev) => (i, twev.mapK(GenEventFeature.ResultEv.toEvent))
        }

    Prop.forAllNoShrink(genFn) {
      case (inputs, twev) =>
        val ev: Event[twev.Type] = twev.evidence
        val atMostOne = Event.amplificationOf(ev).atMostOne

        if (atMostOne) {
          val keys = ev.asKeys

          val dedup = keys
            .preLookup(keys.latest(Duration.Infinite))
            .concatMap {
              case (k, (_, None))    => k :: Nil
              case (_, (_, Some(_))) => Nil
            }

          val ddres =
            Simulator
              .eventToLazyList(dedup, inputs)
              .map(_._2)
              .toList

          val dedupedList =
            Simulator
              .eventToLazyList(ev, inputs)
              .map(_._2)
              .toList
              .distinct

          assertEquals(ddres, dedupedList)
          Prop(ddres == dedupedList)
        } else {
          // with concatMap which can amplify outputs, we might not have exact deduplication
          Prop(true)
        }
    }
  }
}
