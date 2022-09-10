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

import cats.data.{NonEmptyList, WriterT}
import cats.effect.{IO, Timer}
import cats.{Applicative, Defer, Functor, Monad, MonadError, Monoid}
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import scala.concurrent.duration.FiniteDuration

import cats.implicits._
import io.circe.syntax._

class PuaAws(
    invoke: PuaAws.Invoke
) extends SlotEnv {
  // some ID in a data-base we can use
  // to find a pointer to result data
  type SlotId = Long

  /**
    * This tracks a value A and the set of opened
    * slots
    */
  type Slots[A] = PuaAws.Allocator[IO, Long, A]
  implicit def applicativeSlots: Applicative[Slots] =
    PuaAws.Allocator.applicativeAllocator

  type Eff[A] = WriterT[IO, PuaAws.Call, A]
  implicit def monadEff: Monad[Eff] =
    WriterT.catsDataMonadForWriterT

  def allocSlot(p: Pua) = PuaAws.Allocator.allocSlot

  private def dbCall[A <: PuaAws.Action](
      a: A
  )(implicit ev: a.Resp <:< PuaAws.Call, dec: Decoder[a.Resp]): Eff[Unit] =
    WriterT(invoke.dbCall(a).map(resp => (ev(resp), ())))

  // run the lambda as an event-like function with a given
  // location to put the result
  // this is also responsible for updating the state of the db
  // if the arg slot is not yet ready, we update the list of
  // nodes waiting on the arg
  def toFnLater(ln: LambdaFunctionName, arg: SlotId, out: SlotId): Eff[Unit] =
    dbCall(PuaAws.Action.ToCallback(ln, arg, out, None))

  // We have to wait for inputs to be ready for constant
  // functions because we need to order effects correctl;
  def toConst(json: Json, arg: SlotId, out: SlotId): Eff[Unit] =
    dbCall(PuaAws.Action.ToConst(json, arg, out, None))

  // this is really a special job just waits for all the inputs
  // and finally writes to an output
  def makeList(inputs: NonEmptyList[SlotId], output: SlotId): Eff[Unit] =
    dbCall(PuaAws.Action.MakeList(inputs, output, None))

  // opposite of makeList, wait on a slot, then unlist it
  def unList(input: SlotId, outputs: NonEmptyList[SlotId]): Eff[Unit] =
    dbCall(PuaAws.Action.UnList(input, outputs, None))

  /**
    * We assume we can't talk to the DB locally, only the
    * Lambda can, so this sends a message and waits
    * for a reply
    */
  def doAllocation(slots: Int): IO[List[Long]] =
    invoke
      .dbCall(PuaAws.Action.AllocSlots(slots))

  def pollSlot[A: Decoder](slotId: Long): IO[Option[A]] =
    // println(s"in pollSlot($slotId)")
    invoke
      .dbCall(PuaAws.Action.ReadSlot(slotId))
      // .attempt
      // .map { r => println(s"pollSlot($slotId) => $r"); r }
      // .rethrow
      .flatMap {
        case PuaAws.Maybe.Present(PuaAws.Or.First(json)) =>
          IO.fromEither(json.as[A]).map(Some(_))
        case PuaAws.Maybe.Present(PuaAws.Or.Second(err)) => IO.raiseError(err)
        case PuaAws.Maybe.Absent                         => IO.pure(None)
      }

  def completeSlot(slotId: Long, json: Json): IO[Unit] =
    invoke
      .dbCall(
        PuaAws.Action.CompleteSlot(slotId, PuaAws.Or.First(json))
      )
      .flatMap(invoke.handleCall)

  def toIOFn[A: Encoder](
      pua: Pua
  ): A => IO[Long] = {
    val popt = pua.optimize

    val buildSlots = start(popt)

    // println(s"pua = ${pua.asJson.spaces4}\n\n")

    { a: A =>
      val ajson = a.asJson
      val inputPua = Pua.Const(ajson)
      val allSlots = buildSlots.product(allocSlot(inputPua))

      for {
        (fn, slot) <- allSlots.run(doAllocation)
        // _ = println(s"writing $slot = ${ajson.toString.take(30)}...")
        (calls, resSlot) <- fn(slot).run
        // the following can race, due to handleCall being async
        _ <- calls.toList.traverse_(invoke.handleCall)
        _ <- completeSlot(slot, ajson)
        // _ = println(s"result slot = $resSlot")
      } yield resSlot
    }
  }

  def toIOFnPoll[A: Encoder, B: Decoder](pua: Pua, poll: FiniteDuration)(
      implicit timer: Timer[IO]
  ): A => IO[B] = {
    val toSlot = toIOFn[A](pua)

    { a: A =>
      toSlot(a).flatMap { slotId =>
        Defer[IO].fix[B] { recur =>
          pollSlot[B](slotId)
            .flatMap {
              case Some(b) => IO.pure(b)
              case None    =>
                // println(s"$slotId not ready yet")
                timer.sleep(poll) *> recur
            }
        }
      }
    }
  }
}

object PuaAws {
  final case class Allocator[F[_], -T, A] private (
      slots: Int,
      builder: List[T] => F[A]
  ) {
    def map[B](fn: A => B)(implicit F: Functor[F]): Allocator[F, T, B] =
      Allocator(slots, builder.andThen(_.map(fn)))

    def pipeTo[T1 <: T, B](
        that: Allocator[F, T1, A => B]
    )(implicit F: Applicative[F]): Allocator[F, T1, B] =
      Allocator(
        slots + that.slots,
        { sList =>
          val thisSlots = sList.take(slots)
          val thatSlots = sList.drop(slots)
          (builder(thisSlots), that.builder(thatSlots)).mapN { (a, fn) =>
            fn(a)
          }
        }
      )

    def product[T1 <: T, B](
        that: Allocator[F, T1, B]
    )(implicit F: Applicative[F]): Allocator[F, T1, (A, B)] =
      pipeTo(that.map(b => a => (a, b)))

    // given a function that consumes an InsertionOrderSet and returns an F[Map[K, T1]] where
    // each key appears exactly once, return F[A]
    def run[T1 <: T](fn: Int => F[List[T1]])(implicit F: Monad[F]): F[A] =
      fn(slots).flatMap(builder)
  }

  object Allocator {

    def liftF[F[_], A](a: F[A]): Allocator[F, Any, A] =
      Allocator(0, _ => a)

    def pure[F[_], A](a: A)(implicit F: Applicative[F]): Allocator[F, Any, A] =
      liftF(F.pure(a))

    def allocSlot[F[_], A](implicit
        me: MonadError[F, Throwable]
    ): Allocator[F, A, A] =
      Allocator(1, slots => me.catchNonFatal(slots.head))

    implicit def applicativeAllocator[F[_]: Applicative, T]
        : Applicative[Allocator[F, T, *]] =
      new Applicative[Allocator[F, T, *]] {
        def pure[A](a: A): Allocator[F, T, A] = Allocator.pure(a)

        def ap[A, B](
            ff: Allocator[F, T, A => B]
        )(fa: Allocator[F, T, A]): Allocator[F, T, B] =
          fa.pipeTo(ff)
      }
  }

  // Option[A] is dangerous to use with json in circe
  // because null | null will look like a missing value.
  // you can't get Some(null). This has caused a bug before
  sealed abstract class Maybe[+A]
  object Maybe {
    final case object Absent extends Maybe[Nothing]
    final case class Present[A](get: A) extends Maybe[A]

    def fromOption[A](opt: Option[A]): Maybe[A] =
      opt match {
        case Some(a) => Present(a)
        case None    => Absent
      }

    implicit def encoderMaybe[A: Encoder]: Encoder[Maybe[A]] =
      new Encoder[Maybe[A]] {
        val encA = Encoder[A]
        val absentJson = Json.obj()
        def apply(m: Maybe[A]) =
          m match {
            case Present(a) =>
              Json.obj("present" -> encA(a))
            case Absent =>
              absentJson
          }
      }

    implicit def decoderMaybe[A: Decoder]: Decoder[Maybe[A]] =
      new Decoder[Maybe[A]] {
        val absentJson = Json.obj()
        val rightAbs = Right(Maybe.Absent)
        def apply(hc: HCursor) =
          if (hc.value == absentJson) rightAbs
          else
            hc.downField("present").as[A].map(Maybe.Present(_))
      }
  }

  sealed abstract class Call {
    def toList: List[Call.Call1] =
      this match {
        case c1: Call.Call1    => c1 :: Nil
        case Call.Repeated(cs) => cs
      }
  }
  object Call {
    import io.circe.generic.semiauto._

    sealed abstract class Call1 extends Call
    final case class Notification(fn: LambdaFunctionName, arg: Json)
        extends Call1
    final case class Invocation(
        fn: LambdaFunctionName,
        arg: Json,
        resultSlot: Long
    ) extends Call1
    final case class Repeated(items: List[Call1]) extends Call

    val empty: Call = Repeated(Nil)

    def notifications(items: List[(LambdaFunctionName, Json)]): Call =
      Repeated(items.map { case (fn, arg) => Notification(fn, arg) })

    implicit val callMonoid: Monoid[Call] =
      new Monoid[Call] {
        def empty = Call.empty
        def combine(l: Call, r: Call) =
          (l, r) match {
            case (left, Repeated(Nil))          => left
            case (Repeated(Nil), right)         => right
            case (c1: Call1, c2: Call1)         => Repeated(c1 :: c2 :: Nil)
            case (c1: Call1, Repeated(cs))      => Repeated(c1 :: cs)
            case (Repeated(cs), c1: Call1)      => Repeated(cs :+ c1)
            case (Repeated(cs1), Repeated(cs2)) => Repeated(cs1 ::: cs2)
          }
      }
    implicit val encodeCall: Encoder[Call] = {
      val encN = deriveEncoder[Notification]
      val encI = deriveEncoder[Invocation]

      new Encoder[Call] {
        def apply(c: Call): Json =
          c match {
            case n: Notification => encN(n)
            case i: Invocation   => encI(i)
            case Repeated(items) => Json.fromValues(items.map(apply(_)))
          }
      }
    }

    implicit val decodeCall: Decoder[Call] = {
      val decN = deriveDecoder[Notification]
      val decI = deriveDecoder[Invocation]

      implicit val decCall1: Decoder[Call1] =
        decI.widen[Call1].recoverWith { case _ => decN.widen[Call1] }

      new Decoder[Call] {
        def apply(h: HCursor) =
          h.value.asArray match {
            case Some(vs) =>
              vs.traverse(_.as[Call1]).map(vec => Repeated(vec.toList))
            case None => decCall1(h)
          }
      }
    }
  }

  sealed abstract class Action {
    // the type of the response this action gets
    type Resp
  }

  // These are actions that block on some other action
  sealed abstract class BlockingAction extends Action {
    type Resp = Call

    def waitId: Option[Long]
    def withWaitId(wid: Long): Action = {
      import Action._

      this match {
        case const: ToConst => const.copy(waitId = Some(wid))
        case cb: ToCallback => cb.copy(waitId = Some(wid))
        case ul: UnList     => ul.copy(waitId = Some(wid))
        case ml: MakeList   => ml.copy(waitId = Some(wid))
      }
    }
    def argSlots: NonEmptyList[Long]
  }

  object Action {
    final case class ToConst(
        json: Json,
        argSlot: Long,
        resultSlot: Long,
        waitId: Option[Long]
    ) extends BlockingAction {
      def argSlots = NonEmptyList(argSlot, Nil)
    }
    final case class ToCallback(
        name: LambdaFunctionName,
        argSlot: Long,
        resultSlot: Long,
        waitId: Option[Long]
    ) extends BlockingAction {
      def argSlots = NonEmptyList(argSlot, Nil)
    }
    final case class MakeList(
        argSlots: NonEmptyList[Long],
        resultSlot: Long,
        waitId: Option[Long]
    ) extends BlockingAction
    final case class UnList(
        argSlot: Long,
        resultSlots: NonEmptyList[Long],
        waitId: Option[Long]
    ) extends BlockingAction {
      def argSlots = NonEmptyList(argSlot, Nil)
    }

    // create tables
    case object InitTables extends Action {
      type Resp = Unit
    }
    case object CheckTimeouts extends Action {
      type Resp = Call
    }
    final case class AllocSlots(count: Int) extends Action {
      type Resp = List[Long]
    }
    final case class CompleteSlot(slotId: Long, value: Or[Error, Json])
        extends Action {
      type Resp = Call
    }
    final case class ReadSlot(slotId: Long) extends Action {
      type Resp = Maybe[Or[Error, Json]]
    }

    implicit val actionEncoder: Encoder[Action] = {
      import io.circe.generic.semiauto._

      val toConst: Encoder[ToConst] = deriveEncoder[ToConst]
      val toCallback: Encoder[ToCallback] = deriveEncoder[ToCallback]
      val makeList: Encoder[MakeList] = deriveEncoder[MakeList]
      val unList: Encoder[UnList] = deriveEncoder[UnList]
      val allocSlots: Encoder[AllocSlots] = deriveEncoder[AllocSlots]
      val completeSlot: Encoder[CompleteSlot] = deriveEncoder[CompleteSlot]
      val readSlot: Encoder[ReadSlot] = deriveEncoder[ReadSlot]

      new Encoder[Action] {
        def apply(a: Action) =
          a match {
            case tc: ToConst =>
              Json
                .obj("kind" -> Json.fromString("constant"))
                .deepMerge(toConst(tc))
            case tc: ToCallback =>
              Json
                .obj("kind" -> Json.fromString("callback"))
                .deepMerge(toCallback(tc))
            case ml: MakeList =>
              Json
                .obj("kind" -> Json.fromString("makelist"))
                .deepMerge(makeList(ml))
            case ul: UnList =>
              Json
                .obj("kind" -> Json.fromString("unlist"))
                .deepMerge(unList(ul))
            case InitTables =>
              Json.obj("kind" -> Json.fromString("init_tables"))
            case CheckTimeouts =>
              Json.obj("kind" -> Json.fromString("check_timeouts"))
            case as: AllocSlots =>
              Json
                .obj("kind" -> Json.fromString("alloc_slots"))
                .deepMerge(allocSlots(as))
            case cs: CompleteSlot =>
              Json
                .obj("kind" -> Json.fromString("complete_slot"))
                .deepMerge(completeSlot(cs))
            case rs: ReadSlot =>
              Json
                .obj("kind" -> Json.fromString("read_slot"))
                .deepMerge(readSlot(rs))
          }
      }
    }

    implicit val checkTimeoutsDecoder: Decoder[CheckTimeouts.type] =
      new Decoder[CheckTimeouts.type] {
        def apply(a: HCursor) =
          a.downField("kind")
            .as[String]
            .flatMap {
              case "check_timeouts" =>
                Right(CheckTimeouts)
              case unknown =>
                Left(
                  DecodingFailure(
                    s"unexpected kind: $unknown in Action decoding",
                    a.history
                  )
                )
            }
      }

    implicit val actionDecoder: Decoder[Action] = {
      import io.circe.generic.semiauto._

      val toConst: Decoder[ToConst] = deriveDecoder[ToConst]
      val toCallback: Decoder[ToCallback] = deriveDecoder[ToCallback]
      val makeList: Decoder[MakeList] = deriveDecoder[MakeList]
      val unList: Decoder[UnList] = deriveDecoder[UnList]
      val allocSlots: Decoder[AllocSlots] = deriveDecoder[AllocSlots]
      val completeSlot: Decoder[CompleteSlot] = deriveDecoder[CompleteSlot]
      val readSlot: Decoder[ReadSlot] = deriveDecoder[ReadSlot]

      new Decoder[Action] {
        def apply(a: HCursor) =
          a.downField("kind")
            .as[String]
            .flatMap {
              case "constant" =>
                toConst(a)
              case "callback" =>
                toCallback(a)
              case "makelist" =>
                makeList(a)
              case "unlist" =>
                unList(a)
              case "init_tables" =>
                Right(InitTables)
              case "check_timeouts" =>
                Right(CheckTimeouts)
              case "alloc_slots" =>
                allocSlots(a)
              case "complete_slot" =>
                completeSlot(a)
              case "read_slot" =>
                readSlot(a)
              case unknown =>
                Left(
                  DecodingFailure(
                    s"unexpected kind: $unknown in Action decoding",
                    a.history
                  )
                )
            }
      }
    }
  }

  trait Invoke {
    def dbCall[A <: Action](a: A)(implicit dec: Decoder[a.Resp]): IO[a.Resp]
    def handleCall(c: Call): IO[Unit]
  }

  object Invoke {
    def fromSyncAsyncNames(
        syncFn: LambdaFunctionName => Json => IO[Json],
        asyncFn: LambdaFunctionName => Json => IO[Unit],
        dbName: LambdaFunctionName,
        callerName: LambdaFunctionName
    ): Invoke =
      new Invoke {
        def dbCall[A <: Action](
            a: A
        )(implicit dec: Decoder[a.Resp]): IO[a.Resp] =
          syncFn(dbName)((a: Action).asJson).flatMap { resp =>
            IO.fromEither(resp.as[a.Resp])
          }

        def handleCall(c: Call) = asyncFn(callerName)(c.asJson)
      }
  }

  /**
    * This is for json unions where we first try to decode
    * A, then B.
    */
  sealed abstract trait Or[+B, +A] {
    def map[C](fn: A => C): Or[B, C] =
      this match {
        case Or.First(a)  => Or.First(fn(a))
        case Or.Second(b) => Or.Second(b)
      }

    def toEither: Either[B, A] =
      this match {
        case Or.First(a)  => Right(a)
        case Or.Second(b) => Left(b)
      }
  }
  object Or {
    final case class First[+A](first: A) extends Or[Nothing, A]
    final case class Second[+B](second: B) extends Or[B, Nothing]

    implicit def encodeOr[B, A](implicit
        encB: Encoder[B],
        encA: Encoder[A]
    ): Encoder[Or[B, A]] =
      new Encoder[Or[B, A]] {
        def apply(or: Or[B, A]) =
          or match {
            case First(a)  => encA(a)
            case Second(b) => encB(b)
          }
      }

    implicit def decodeOr[B, A](implicit
        decB: Decoder[B],
        decA: Decoder[A]
    ): Decoder[Or[B, A]] = {
      val secDec = decB.map(Second(_): Or[B, A])
      decA
        .map(First(_): Or[B, A])
        .recoverWith { case _ => secDec }
    }
  }

  final case class Error(code: Int, message: String) extends Exception(message)
  object Error {
    import io.circe.generic.semiauto._
    implicit val errorDecoder: Decoder[Error] = deriveDecoder[Error]
    implicit val errorEncoder: Encoder[Error] = deriveEncoder[Error]

    val InvocationError: Int = 1
    val InvalidUnlistLength: Int = 2

    def fromInvocationError(err: Throwable): Error =
      Error(InvocationError, err.getMessage())

    def fromEither[A](eit: Either[Throwable, A]): Or[Error, A] =
      eit match {
        case Left(e)  => Or.Second(fromInvocationError(e))
        case Right(a) => Or.First(a)
      }
  }
}
