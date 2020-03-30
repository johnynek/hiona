package dev.posco.hiona

import cats.effect.{ContextShift, IO}

import cats.implicits._

class EmitterTests extends munit.FunSuite {
  implicit val ec = scala.concurrent.ExecutionContext.global

  implicit val ctx: ContextShift[IO] = IO.contextShift(ec)

  def result[A](ev: Event[A], feeder: IO[Feeder]): IO[List[A]] = {
    val emitterIO = Engine.Emitter.fromEvent(ev)
    (feeder, emitterIO).mapN(Engine.Emitter.runToList(_, 100, _)).flatten
  }

  def resultLabel[A, B](ev: LabeledEvent[A, B], feeder: IO[Feeder]): IO[List[(A, B)]] = {
    val emitterIO = Engine.Emitter.fromLabeledEvent(ev)
    (feeder, emitterIO).mapN(Engine.Emitter.runToList(_, 100, _)).flatten
  }


  test("basic map/event processing") {
    val src = Event.source[(Long, Int)]("numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      })

    val data = List((0L, 42), (1L, 43), (2L, 44))

    val feeder = Feeder.iterableFeeder(src, Duration.zero, data)

    val res = src.map { case (_, i) => i.toString }

    result(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(lst, List("42", "43", "44"))
        }
      }
      .unsafeToFuture()
  }

  test("lookupBefore test") {
    val src = Event.source[(Long, Int)]("numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      })
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))

    val feeder = Feeder.iterableFeeder(src, Duration.zero, data)


    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = keys.lookupBefore(feat)

    result(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(lst, List((0, ((), 0)), (1, ((), 0)), (0, ((), 42)), (1, ((), 43))))
        }
      }
      .unsafeToFuture()
  }

  test("lookupAfter test") {
    val src = Event.source[(Long, Int)]("numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      })
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))

    val feeder = Feeder.iterableFeeder(src, Duration.zero, data)


    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = keys.lookupAfter(feat)

    result(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(lst, List((0, ((), 42)), (1, ((), 43)), (0, ((), 86)), (1, ((), 88))))
        }
      }
      .unsafeToFuture()
  }

  test("simple LabeledEvent test") {
    val src = Event.source[(Long, Int)]("numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      })
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))

    val feeder =
      List(Duration.zero, Duration(3L))
        .traverse { d =>
          Feeder.iterableFeeder(src, d, data)
        }
        .flatMap(Feeder.multiFeeder(_))

    val label = Label(src.map { case (_, i) => (i % 2, i) }.sum).lookForward(Duration(3L))
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = LabeledEvent(keys, label)

    resultLabel(res, feeder)
      .flatMap { lst =>
        IO {
          assertEquals(lst, List((0, ((), 86)), (1, ((), 88)), (0, ((), 86)), (1, ((), 88))))
        }
      }
      .unsafeToFuture()
  }
}
