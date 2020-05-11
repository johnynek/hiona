package dev.posco.hiona

/**
  * This is an in-memory implementation of hiona that is useful for
  * testing other implementations against
  */
object Simulator {
  case class InputData[A](inputs: List[A], source: Event.Source[A])

  type Inputs = List[TypeWith[InputData]]

  def eventToLazyList[A](ev: Event[A], inputs: Inputs): LazyList[(Point, A)] =
    ev match {
      case src @ Event.Source(_, _, _) =>
        val key = Point.Key(src.name, Duration.zero)

        val good: List[TypeWith.Aux[InputData, _ <: A]] = inputs
          .map { tw =>
            Event.Source.equiv(tw.evidence.source, src) match {
              case Some(cast) =>
                type T[+Z] = TypeWith.Aux[InputData, _ <: Z]
                val twA: T[A] = cast.substituteCo[T](tw)
                Some(twA)
              case None =>
                None
            }
          }
          .collect { case Some(r) => r }

        good match {
          case tw :: Nil =>
            // we know tw.Type <: A
            val ins: List[tw.Type] = tw.evidence.inputs
            val valid: Validator[tw.Type] = tw.evidence.source.validator

            val withTs: LazyList[(Point, tw.Type)] =
              ins
                .to(LazyList)
                .map { a =>
                  valid.validate(a) match {
                    case Right(ts) =>
                      (
                        Point.Sourced[tw.Type](tw.evidence.source, a, ts, key),
                        a
                      )
                    case Left(err) =>
                      throw new Exception(
                        s"in source: ${tw.evidence.source.name}",
                        err
                      )
                  }
                }

            withTs
          case notOne =>
            sys.error(s"missing: ${src.name}, from: ${inputs
              .map(_.evidence.source.name)}, we have: ${notOne.size} matches")
        }
      case Event.Empty => LazyList.empty
      case Event.ConcatMapped(orig, fn) =>
        eventToLazyList(orig, inputs)
          .flatMap {
            case (p, a0) =>
              fn(a0).map((p, _))
          }
      case Event.Mapped(orig, fn) =>
        eventToLazyList(orig, inputs)
          .map {
            case (p, a0) =>
              (p, fn(a0))
          }
      case Event.WithTime(ev, cast) =>
        val res = eventToLazyList(ev, inputs)
          .map {
            case (point, a) =>
              (point, (a, point.ts))
          }
        cast.substituteCo[Lambda[x => LazyList[(Point, x)]]](res)
      case Event.Filtered(ev, fn, cast) =>
        val res = eventToLazyList(ev, inputs).filter { case (_, x) => fn(x) }
        cast.substituteCo[Lambda[x => LazyList[(Point, x)]]](res)
      case Event.ValueWithTime(ev, cast) =>
        val res = eventToLazyList(ev, inputs).map {
          case (p, (k, v)) =>
            (p, (k, (v, p.ts)))
        }
        cast.substituteCo[Lambda[x => LazyList[(Point, x)]]](res)
      case Event.Concat(left, right) =>
        val rleft = eventToLazyList(left, inputs)
        val rright = eventToLazyList(right, inputs)
        merge(rleft, rright)(Ordering[Point].on[(Point, A)](_._1))
      case Event.Lookup(ev, feat, order) =>
        val lookupFn = featureAt(feat, order, inputs)

        eventToLazyList(ev, inputs).map {
          case (p, (k, v)) =>
            val w = lookupFn(p, k)
            (p, (k, (v, w)))
        }
    }

  @annotation.tailrec
  private def getPoint[A](
      p: Point,
      ord: LookupOrder,
      items: LazyList[(Point, (A, A))],
      prev: Option[(A, A)]
  ): Option[A] =
    items match {
      case (p0, as0) #:: _ if ord.isBefore && (p0.ts == p.ts) => Some(as0._1)
      case (_, as0) #:: (rest @ ((p1, _) #:: _))
          if ord.isAfter && (p1.ts == p.ts) =>
        getPoint(p, ord, rest, Some(as0))

      case (p0, as) #:: tail =>
        if (Ordering[Timestamp].lteq(p0.ts, p.ts)) {
          val newAcc = Some(as)
          getPoint(p, ord, tail, newAcc)
        } else {
          // p.ts < p0.ts
          // we know there is no timestamp where they
          // are the same, so the before case is the same as the after
          Some(as._1)
        }
      case LazyList() =>
        prev.map(_._2)
    }

  def featureAt[K, V](
      feat: Feature[K, V],
      ord: LookupOrder,
      inputs: Inputs
  ): (Point, K) => V =
    feat match {
      case Feature.Summed(ev, mon) =>
        val allKvs = eventToLazyList(ev, inputs)
        val summed = allKvs
          .scanLeft((Option.empty[Point], (Map.empty[K, V], Map.empty[K, V]))) {
            case ((_, (_, prev)), (p, (k, v))) =>
              val next = prev.get(k) match {
                case None     => prev.updated(k, v)
                case Some(v0) => prev.updated(k, mon.combine(v0, v))
              }
              (Some(p), (prev, next))
          }
          .collect { case (Some(p), pair) => (p, pair) }

        { (p, k) =>
          getPoint(p, ord, summed, None)
            .flatMap(_.get(k))
            .getOrElse(mon.empty)
        }
      case l @ Feature.Latest(_, _, _) =>
        def go[W](l: Feature.Latest[K, W, V]): (Point, K) => V = {
          // TODO: support durations
          val allKvs = eventToLazyList(l.event, inputs)
          val latest =
            allKvs
              .scanLeft(
                (Option.empty[Point], (Map.empty[K, W], Map.empty[K, W]))
              ) {
                case ((_, (_, prev)), (p, (k, v))) =>
                  val next = prev.updated(k, v)
                  (Some(p), (prev, next))
              }
              .collect { case (Some(p), pair) => (p, pair) }

          { (p, k) =>
            val optW =
              getPoint(p, ord, latest, None)
                .flatMap(_.get(k))

            l.cast(optW)
          }
        }

        go(l)
      case Feature.Mapped(feat, fn) =>
        val lookup = featureAt(feat, ord, inputs)

        { (p, k) => fn(k, lookup(p, k), p.ts) }
      case Feature.Zipped(left, right, cast) =>
        val llu = featureAt(left, ord, inputs)
        val rlu = featureAt(right, ord, inputs)

        { (p, k) => cast((llu(p, k), rlu(p, k))) }
    }

  def merge[A](left: LazyList[A], right: LazyList[A])(
      implicit ord: Ordering[A]
  ): LazyList[A] =
    left match {
      case lh #:: lt =>
        right match {
          case rh #:: rt =>
            if (ord.lteq(lh, rh)) lh #:: merge(lt, right)
            else rh #:: merge(left, rt)
          case LazyList() => left
        }
      case LazyList() => right
    }
}
