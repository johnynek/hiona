package dev.posco.hiona.aws

import cats.{Applicative, Monad}
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.effect.concurrent.{Deferred, Ref}
import io.circe.{Decoder, Encoder, Json}

import cats.implicits._

final class PuaLocal(
    ref: Ref[IO, (Long, Map[Long, Deferred[IO, Either[Throwable, Json]]])],
    run: LambdaFunctionName => (Json => IO[Json])
)(implicit ctx: ContextShift[IO])
    extends SlotEnv {
  type SlotId = Long

  /**
    * This tracks a value A and the set of opened
    * slots
    */
  type Slots[A] = IO[A]
  def applicativeSlots: Applicative[Slots] = cats.effect.Effect[IO]
  type Eff[A] = IO[A]
  def monadEff: Monad[Slots] = cats.effect.Effect[IO]

  private def readSlot(s: SlotId): IO[Json] =
    for {
      (_, slots) <- ref.get
      jsonD <- slots.get(s) match {
        case None =>
          IO.raiseError(new IllegalStateException(s"no $s in $slots"))
        case Some(d) => IO.pure(d)
      }
      jsonTry <- jsonD.get
      json <- IO.fromEither(jsonTry)
    } yield json

  private def failSlot(s: SlotId, err: Throwable): IO[Unit] =
    for {
      (_, slots) <- ref.get
      jsonD <- slots.get(s) match {
        case None =>
          IO.raiseError(new IllegalStateException(s"no $s in $slots"))
        case Some(d) => IO.pure(d)
      }
      _ <- jsonD.complete(Left(err))
    } yield ()

  // run the lambda as an event-like function with a given
  // location to put the result
  // this is also responsible for updating the state of the db
  // if the arg slot is not yet ready, we update the list of
  // nodes waiting on the arg
  def toFnLater(ln: LambdaFunctionName, arg: SlotId, out: SlotId): IO[Unit] = {
    val fn = run(ln)

    val computation =
      for {
        jin <- readSlot(arg)
        jout <- fn(jin)
        _ <- writeSlot(jout, out)
      } yield ()

    val recovered = computation.onError { case t => failSlot(out, t) }

    // TODO: I don't think this is dangerous, but we are discarding
    // the fiber so we can't join or cancel, seems like code-smell
    // but that is what the lambda will do
    recovered.start.void
  }

  def toConst(json: Json, arg: SlotId, out: SlotId): Slots[Unit] = {
    // we need to make a slot that is only ready when the input is read
    val computation =
      for {
        _ <- readSlot(arg)
        _ <- writeSlot(json, out)
      } yield ()

    val recovered = computation.onError { case t => failSlot(out, t) }

    recovered.start.void
  }

  // This creates a new entry in the database for an output location
  // only one item should ever write to it
  def allocSlot: Slots[SlotId] =
    ref.access
      .flatMap {
        case ((nextId, slots), set) =>
          for {
            d <- Deferred[IO, Either[Throwable, Json]]
            slots1 = slots.updated(nextId, d)
            success <- set((nextId + 1L, slots1))
            slot <- if (success) IO.pure(nextId) else allocSlot
          } yield slot
      }

  // this is really a special job just waits for all the inputs
  // and finally writes to an output
  def makeList(inputs: NonEmptyList[SlotId], output: SlotId): IO[Unit] =
    inputs
      .traverse(readSlot)
      .onError { case t => failSlot(output, t) }
      .flatMap(js => writeSlot(Json.fromValues(js.toList), output))
      .start
      .void // TODO: codesmell, fire/forget

  // opposite of makeList, wait on a slot, then unlist it
  def unList(input: SlotId, outputs: NonEmptyList[SlotId]): IO[Unit] =
    readSlot(input)
      .onError { case t => outputs.traverse_(failSlot(_, t)) }
      .flatMap { j =>
        j.asArray match {
          case Some(vs) if vs.size == outputs.size =>
            vs.toList
              .zip(outputs.toList)
              .traverse_ { case (j, s) => writeSlot(j, s) }
          case _ =>
            val err = new IllegalStateException(
              s"expected json array of size: ${outputs.size} from input: $input, found: $j"
            )
            outputs.traverse_(failSlot(_, err)) *> IO.raiseError(err)
        }
      }
      .start // TODO: same code smell with starting fire/forget as above
      .void

  def writeSlot(j: Json, slot: SlotId): IO[Unit] =
    for {
      (_, slots) <- ref.get
      d <- slots.get(slot) match {
        case None =>
          IO.raiseError(new IllegalStateException(s"unknown slot: $slot"))
        case Some(d) => IO.pure(d)
      }
      _ <- d.complete(Right(j))
    } yield ()

  def apply[A: Encoder, B: Decoder](p: Pua): A => IO[B] = {
    val makeFn = start(p.optimize)

    { a: A =>
      for {
        fn <- makeFn
        inslot <- allocSlot
        _ <- writeSlot(Encoder[A].apply(a), inslot)
        out <- fn(inslot)
        json <- readSlot(out)
        b <- json.as[B] match {
          case Right(b)  => IO.pure(b)
          case Left(err) => IO.raiseError(err)
        }
      } yield b
    }
  }
}

object PuaLocal {
  def build(
      runFn: LambdaFunctionName => (Json => IO[Json])
  )(implicit ctx: ContextShift[IO]): IO[PuaLocal] =
    Ref
      .of[IO, (Long, Map[Long, Deferred[IO, Either[Throwable, Json]]])](
        (0L, Map.empty)
      )
      .map(new PuaLocal(_, runFn))

  val emptyDirectory: Directory = Directory(Map.empty)

  case class Directory(toMap: Map[String, Json => IO[Json]])
      extends Function1[LambdaFunctionName, (Json => IO[Json])] {

    def unknown(n: LambdaFunctionName): Json => IO[Json] = { j: Json =>
      IO.raiseError(
        new IllegalStateException(s"unknown function: $n called with arg: $j")
      )
    }

    def apply(n: LambdaFunctionName): Json => IO[Json] =
      toMap.get(n.asString) match {
        case Some(fn) => fn
        case None     => unknown(n)
      }

    def addFn[A: Decoder, B: Encoder](
        name: String,
        fn: A => IO[B]
    ): Directory = {
      val rawFn = { j: Json =>
        j.as[A] match {
          case Right(a)  => fn(a).map(Encoder[B].apply(_))
          case Left(err) => IO.raiseError(err)
        }
      }

      Directory(toMap.updated(name, rawFn))
    }
  }
}
