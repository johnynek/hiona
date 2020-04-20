package dev.posco.hiona

import cats.effect.{ContextShift, IO}

import cats.implicits._

object EmitterTests {

  abstract class FromIterable {
    type A
    val event: Event.Source[A]
    val iterable: Iterable[A]
  }

  object FromIterable {
    def apply[A0](
        ev: Event.Source[A0],
        iter: Iterable[A0]
    ): FromIterable { type A = A0 } =
      new FromIterable {
        type A = A0
        val event = ev
        val iterable = iter
      }
  }

  def feederFor(
      its: Iterable[FromIterable],
      feeds: Map[String, (Set[Event.Source[_]], Set[Duration])]
  ): IO[Feeder] =
    its.toList
      .traverse { fromIt =>
        val (srcs, durs) = feeds(fromIt.event.name)

        assert(srcs == Set(fromIt.event))

        durs.toList.sorted.traverse { dur =>
          Feeder.iterableFeeder[fromIt.A](fromIt.event, dur, fromIt.iterable)
        }
      }
      .flatMap(llf => Feeder.multiFeeder(llf.flatten))

  def feederForEvent[A](its: Iterable[FromIterable], ev: Event[A]): IO[Feeder] =
    feederFor(its, Event.sourcesOf(ev).map {
      case (k, v) => (k, (v, Set(Duration.Zero)))
    })

  def feederForLabeledEvent[A](
      its: Iterable[FromIterable],
      ev: LabeledEvent[A]
  ): IO[Feeder] =
    feederFor(its, LabeledEvent.sourcesAndOffsetsOf(ev))
}

class EmitterTests extends munit.FunSuite {
  implicit val ec = scala.concurrent.ExecutionContext.global

  implicit val ctx: ContextShift[IO] = IO.contextShift(ec)

  def result[A](ev: Event[A], feeder: IO[Feeder]): IO[List[A]] = {
    val emitterIO = Engine.Emitter.fromEvent(ev)
    (feeder, emitterIO).mapN(Engine.Emitter.runToList(_, 100, _)).flatten
  }

  def resultLabel[A](
      ev: LabeledEvent[A],
      feeder: IO[Feeder]
  ): IO[List[A]] = {
    val emitterIO = Engine.Emitter.fromLabeledEvent(ev)
    (feeder, emitterIO).mapN(Engine.Emitter.runToList(_, 100, _)).flatten
  }

  test("basic map/event processing") {
    val src = Event.source[(Long, Int)]("numsrc", new Validator[(Long, Int)] {
      def validate(v: (Long, Int)) = Right(Timestamp(v._1))
    })

    val data = List((0L, 42), (1L, 43), (2L, 44))
    val fromIters = List(EmitterTests.FromIterable(src, data))

    val res = src.map { case (_, i) => i.toString }
    val feeder =
      EmitterTests.feederForEvent(fromIters, res)

    result(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(lst, List("42", "43", "44"))
        }
      }
      .unsafeToFuture()
  }

  test("preLookup test") {
    val src = Event.source[(Long, Int)]("numsrc", new Validator[(Long, Int)] {
      def validate(v: (Long, Int)) = Right(Timestamp(v._1))
    })
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))

    val fromIters = List(EmitterTests.FromIterable(src, data))

    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = keys.preLookup(feat)

    val feeder =
      EmitterTests.feederForEvent(fromIters, res)

    result(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List((0, ((), 0)), (1, ((), 0)), (0, ((), 42)), (1, ((), 43)))
          )
        }
      }
      .unsafeToFuture()
  }

  test("postLookup test") {
    val src = Event.source[(Long, Int)]("numsrc", new Validator[(Long, Int)] {
      def validate(v: (Long, Int)) = Right(Timestamp(v._1))
    })
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))
    val fromIters = List(EmitterTests.FromIterable(src, data))

    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = keys.postLookup(feat)

    val feeder =
      EmitterTests.feederForEvent(fromIters, res)

    result(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List((0, ((), 42)), (1, ((), 43)), (0, ((), 86)), (1, ((), 88)))
          )
        }
      }
      .unsafeToFuture()
  }

  test("simple LabeledEvent test") {
    val src = Event.source[(Long, Int)]("numsrc", new Validator[(Long, Int)] {
      def validate(v: (Long, Int)) = Right(Timestamp(v._1))
    })
    val data = List((0L, 1), (1L, 2), (2L, 3), (3L, 4), (4L, 5), (5L, 6))

    val fromIters = List(EmitterTests.FromIterable(src, data))

    val label =
      Label(src.map { case (_, i) => (i % 2, i) }.sum).lookForward(Duration(3L))
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = LabeledEvent(keys, label)

    val feeder =
      EmitterTests.feederForLabeledEvent(fromIters, res)

    resultLabel(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List(
              (1, ((), 4)),
              (0, ((), 6)),
              (1, ((), 9)),
              (0, ((), 12)),
              (1, ((), 9)),
              (0, ((), 12))
            )
          )
        }
      }
      .unsafeToFuture()
  }

  test("simple zipped LabeledEvent test") {
    val src = Event.source[(Long, Int)]("numsrc", new Validator[(Long, Int)] {
      def validate(v: (Long, Int)) = Right(Timestamp(v._1))
    })
    val data = List((0L, 1), (1L, 2), (2L, 3), (3L, 4), (4L, 5), (5L, 6))

    val fromIters = List(EmitterTests.FromIterable(src, data))

    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    // exercise a lookahead where an read and write collide,
    // we should see the feature value at the end of the
    // timestamp
    val label0 = Label(feat).lookForward(Duration(2L))
    val label1 = Label(feat).lookForward(Duration(4L))
    val label = label0.zip(label1)
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = LabeledEvent(keys, label)

    val feeder =
      EmitterTests.feederForLabeledEvent(fromIters, res)

    resultLabel(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List(
              (1, ((), (4, 9))),
              (0, ((), (6, 12))),
              (1, ((), (9, 9))),
              (0, ((), (12, 12))),
              (1, ((), (9, 9))),
              (0, ((), (12, 12)))
            )
          )
        }
      }
      .unsafeToFuture()
  }
}
