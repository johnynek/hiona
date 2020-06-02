package dev.posco.hiona.aws

import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, IO, LiftIO, Timer}
import cats.{Applicative, Defer, Functor, Monad, MonadError}
import com.amazonaws.services.lambda.runtime.Context
import doobie.ConnectionIO
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import org.postgresql.util.PSQLException
import scala.concurrent.duration.FiniteDuration
import java.sql.SQLTransientException

import cats.implicits._
import doobie.implicits._
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

  type Eff[A] = IO[A]
  implicit def monadEff: Monad[Eff] =
    cats.effect.Effect[IO]

  def allocSlot(p: Pua) = PuaAws.Allocator.allocSlot

  // run the lambda as an event-like function with a given
  // location to put the result
  // this is also responsible for updating the state of the db
  // if the arg slot is not yet ready, we update the list of
  // nodes waiting on the arg
  def toFnLater(ln: LambdaFunctionName, arg: SlotId, out: SlotId): Eff[Unit] =
    invoke.async(PuaAws.Action.ToCallback(ln, arg, out, None))

  // We have to wait for inputs to be ready for constant
  // functions because we need to order effects correctl;
  def toConst(json: Json, arg: SlotId, out: SlotId): Eff[Unit] =
    invoke.async(PuaAws.Action.ToConst(json, arg, out, None))

  // this is really a special job just waits for all the inputs
  // and finally writes to an output
  def makeList(inputs: NonEmptyList[SlotId], output: SlotId): Eff[Unit] =
    invoke.async(PuaAws.Action.MakeList(inputs, output, None))

  // opposite of makeList, wait on a slot, then unlist it
  def unList(input: SlotId, outputs: NonEmptyList[SlotId]): Eff[Unit] =
    invoke.async(PuaAws.Action.UnList(input, outputs, None))

  /**
    * We assume we can't talk to the DB locally, only the
    * Lambda can, so this sends a message and waits
    * for a reply
    */
  def doAllocation(slots: Int): IO[List[Long]] =
    invoke
      .sync(PuaAws.Action.AllocSlots(slots))
      .map { slotList =>
        //println(s"allocating: $slots slots = $slotList")
        slotList
      }

  def pollSlot[A: Decoder](slotId: Long): IO[Option[A]] =
    //println(s"in pollSlot($slotId)")
    invoke
      .sync(PuaAws.Action.ReadSlot(slotId))
      // .attempt
      // .map { r => println(s"pollSlot($slotId) => $r"); r }
      // .rethrow
      .flatMap {
        case PuaAws.Maybe.Present(PuaAws.Error.NotError(json)) =>
          IO.fromEither(json.as[A]).map(Some(_))
        case PuaAws.Maybe.Present(err: PuaAws.Error) => IO.raiseError(err)
        case PuaAws.Maybe.Absent                     => IO.pure(None)
      }

  def completeSlot(slotId: Long, json: Json): IO[Unit] =
    invoke.async(PuaAws.Action.CompleteSlot(slotId, json))

  def toIOFn[A: Encoder](
      pua: Pua
  ): A => IO[Long] = {
    val popt = pua.optimize

    val buildSlots = start(popt)

    //println(s"pua = ${pua.asJson.spaces4}\n\n")

    { a: A =>
      val ajson = a.asJson
      val inputPua = Pua.Const(ajson)
      val allSlots = buildSlots.product(allocSlot(inputPua))

      for {
        (fn, slot) <- allSlots.run(doAllocation)
        //_ = println(s"writing $slot = ${ajson.toString.take(30)}...")
        _ <- completeSlot(slot, ajson)
        resSlot <- fn(slot)
        //_ = println(s"result slot = $resSlot")
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
                //println(s"$slotId not ready yet")
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

  sealed abstract class Action {
    // the type of the response this action gets
    type Resp
  }

  // These are actions that block on some other action
  sealed abstract class BlockingAction extends Action {
    type Resp = Unit

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
      type Resp = Unit
    }
    final case class AllocSlots(count: Int) extends Action {
      type Resp = List[Long]
    }
    final case class CompleteSlot(slotId: Long, value: Json) extends Action {
      type Resp = Unit
    }
    final case class ReadSlot(slotId: Long) extends Action {
      type Resp = Maybe[Error.Or[Json]]
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
    def sync[A <: Action](a: A)(implicit dec: Decoder[a.Resp]): IO[a.Resp]
    def async(a: Action): IO[Unit]
  }

  object Invoke {
    def fromSyncAsync(
        syncFn: Json => IO[Json],
        asyncFn: Json => IO[Unit]
    ): Invoke =
      new Invoke {
        def sync[A <: Action](a: A)(implicit dec: Decoder[a.Resp]): IO[a.Resp] =
          syncFn((a: Action).asJson).flatMap { resp =>
            IO.fromEither(resp.as[a.Resp])
          }

        def async(a: Action): IO[Unit] = asyncFn(a.asJson)
      }
  }

  final case class Error(code: Int, message: String)
      extends Exception(message)
      with Error.Or[Nothing]
  object Error {
    import io.circe.generic.semiauto._
    implicit val errorDecoder: Decoder[Error] = deriveDecoder[Error]
    implicit val errorEncoder: Encoder[Error] = deriveEncoder[Error]

    sealed abstract trait Or[+A] {
      def map[B](fn: A => B): Or[B] =
        this match {
          case e @ Error(_, _) => e
          case NotError(a)     => NotError(fn(a))
        }

      def toEither: Either[Error, A] =
        this match {
          case e @ Error(_, _) => Left(e)
          case NotError(a)     => Right(a)
        }
    }
    final case class NotError[A](value: A) extends Error.Or[A]
    object Or {
      implicit def encoderOr[A: Encoder]: Encoder[Or[A]] =
        new Encoder[Or[A]] {
          val aEnc: Encoder[A] = Encoder[A]

          def apply(o: Or[A]) =
            o match {
              case e @ Error(_, _) => errorEncoder(e)
              case NotError(a)     => aEnc(a)
            }
        }

      implicit def decoderOr[A: Decoder]: Decoder[Or[A]] = {
        val notA = Decoder[A].map(NotError(_): Or[A])
        errorDecoder.widen[Or[A]].handleErrorWith(_ => notA)
      }
    }

    val InvocationError: Int = 1
    val InvalidUnlistLength: Int = 2

    def fromInvocationError(err: Throwable): Error =
      Error(InvocationError, err.getMessage())

    def fromEither[A](eit: Either[Throwable, A]): Or[A] =
      eit match {
        case Left(e)  => fromInvocationError(e)
        case Right(a) => NotError(a)
      }
  }

  final case class State(
      dbControl: DBControl,
      makeLambda: LambdaFunctionName => Json => IO[Json],
      makeAsyncLambda: LambdaFunctionName => Json => IO[Unit],
      blocker: Blocker,
      ctxShift: ContextShift[IO],
      timer: Timer[IO]
  )

  def buildLambdaState: IO[State] = ???
}

abstract class DBControl {

  def initializeTables: ConnectionIO[Unit]

  def cleanupTables: ConnectionIO[Unit]

  def run[A](c: ConnectionIO[A]): IO[A]

  def allocSlots(count: Int): ConnectionIO[List[Long]]

  def readSlot(slotId: Long): ConnectionIO[Option[PuaAws.Error.Or[Json]]]

  def addWaiter(
      act: PuaAws.BlockingAction,
      function: LambdaFunctionName
  ): ConnectionIO[Unit]

  def removeWaiter(act: PuaAws.BlockingAction): ConnectionIO[Unit]

  def completeSlot(
      slotId: Long,
      result: PuaAws.Error.Or[Json],
      invoker: LambdaFunctionName => Json => IO[Unit]
  ): ConnectionIO[IO[Unit]]

  def resendNotifications(
      invoker: LambdaFunctionName => Json => IO[Unit]
  ): ConnectionIO[IO[Unit]]
}

class PuaWorker extends PureLambda[PuaAws.State, PuaAws.Action, Json] {
  import PuaAws.Action._
  import PuaAws.State

  def setup = PuaAws.buildLambdaState

  def toConst(
      tc: ToConst,
      st: State,
      context: Context
  ): ConnectionIO[IO[Unit]] = {
    import st.dbControl

    dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(either) =>
          // data or error exists, so, we can write the data
          val thisRes = either.map(_ => tc.json)
          for {
            action <-
              dbControl.completeSlot(tc.resultSlot, thisRes, st.makeAsyncLambda)
            _ <- dbControl.removeWaiter(tc)
          } yield action
        case None =>
          //println(s"$tc could not read")
          // we have to wait and callback.
          dbControl
            .addWaiter(
              tc,
              LambdaFunctionName(context.getInvokedFunctionArn())
            )
            .as(IO.unit)
      }
  }

  /**
    *
   * if the slot isn't complete, wait, else fire off the given callback
    * we can't hold the transaction while we are blocking on the call here
    * in the future, we could set up possibly an alias to configure async
    * calls to the target, but that seems to change the calling semantics,
    * maybe better just to use the smallest lambda for now and keep sync
    * calls. We can revisit
    */
  def toCallback(
      tc: ToCallback,
      st: State,
      context: Context
  ): ConnectionIO[IO[Unit]] = {
    // do the finishing transaction, including removing the waiter
    def write(json: Either[Throwable, Json]): IO[Unit] = {
      val res = PuaAws.Error.fromEither(json)
      st.dbControl
        .run(for {
          next <-
            st.dbControl.completeSlot(tc.resultSlot, res, st.makeAsyncLambda)
          _ <- st.dbControl.removeWaiter(tc)
        } yield next)
        .flatten
    }

    st.dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(PuaAws.Error.NotError(json)) =>
          // the slot is complete, we can now fire off an event:
          val fn = st.makeLambda(tc.name)
          val invoke = fn(json).attempt.flatMap(write)
          Applicative[ConnectionIO].pure(invoke)
        case Some(err @ PuaAws.Error(_, _)) =>
          st.dbControl.completeSlot(tc.resultSlot, err, st.makeAsyncLambda) <*
            st.dbControl.removeWaiter(tc)
        case None =>
          //println(s"$tc could not read")
          // we have to wait and callback.
          st.dbControl
            .addWaiter(tc, LambdaFunctionName(context.getInvokedFunctionArn()))
            .as(IO.unit)
      }
  }

  def makeList(
      ml: MakeList,
      st: State,
      context: Context
  ): ConnectionIO[IO[Unit]] = {
    import st.dbControl

    ml.argSlots
      .traverse(dbControl.readSlot(_))
      .flatMap { neOpts =>
        neOpts.sequence match {
          case Some(nelJson) =>
            // all the inputs are done, we can write
            val write = nelJson.traverse(_.toEither).map { noErrors =>
              Json.fromValues(noErrors.toList)
            }
            dbControl.completeSlot(
              ml.resultSlot,
              PuaAws.Error.fromEither(write),
              st.makeAsyncLambda
            ) <*
              dbControl.removeWaiter(ml)

          case None =>
            // we are not done, continue to wait
            dbControl
              .addWaiter(
                ml,
                LambdaFunctionName(context.getInvokedFunctionArn())
              )
              .as(IO.unit)
        }
      }
  }

  def unList(
      ul: UnList,
      st: State,
      context: Context
  ): ConnectionIO[IO[Unit]] = {
    import st.dbControl

    dbControl
      .readSlot(ul.argSlot)
      .flatMap {
        case Some(done) =>
          val write = done match {
            case PuaAws.Error.NotError(success) =>
              val resSize = ul.resultSlots.size
              success.asArray match {
                case Some(v) if v.size == resSize =>
                  ul.resultSlots.toList.toVector
                    .zip(v)
                    .traverse {
                      case (slot, json) =>
                        dbControl.completeSlot(
                          slot,
                          PuaAws.Error.NotError(json),
                          st.makeAsyncLambda
                        )
                    }
                    .map(_.sequence_)
                case _ =>
                  val err = PuaAws.Error(
                    PuaAws.Error.InvalidUnlistLength,
                    s"expected a list of size: $resSize, got: ${success.noSpaces}"
                  )

                  ul.resultSlots
                    .traverse { slot =>
                      dbControl.completeSlot(
                        slot,
                        err,
                        st.makeAsyncLambda
                      )
                    }
                    .map(_.sequence_)
              }
            case err @ PuaAws.Error(_, _) =>
              // set all the downstreams as failures
              ul.resultSlots
                .traverse { slot =>
                  dbControl.completeSlot(slot, err, st.makeAsyncLambda)
                }
                .map(_.sequence_)
          }

          write <* dbControl.removeWaiter(ul)
        case None =>
          //println(s"$ul could not read")
          // we are not done, continue to wait
          dbControl
            .addWaiter(
              ul,
              LambdaFunctionName(context.getInvokedFunctionArn())
            )
            .as(IO.unit)
      }
  }

  def run(input: IO[PuaAws.Action], st: State, context: Context): IO[Json] = {
    val rng = new java.util.Random(context.getAwsRequestId().hashCode)
    val nextDouble: IO[Double] = IO(rng.nextDouble)

    def nextSleep(retry: Int): IO[Unit] =
      for {
        d <- nextDouble
        dr = retry * 100.0 * d
        dur = FiniteDuration(dr.toLong, "ms")
        _ <- st.timer.sleep(dur)
      } yield ()

    def encode[A <: PuaAws.Action](a: A)(
        resp: ConnectionIO[IO[a.Resp]]
    )(implicit enc: Encoder[a.Resp]): ConnectionIO[IO[Json]] =
      resp.map(_.map(_.asJson))

    def process(a: PuaAws.Action): ConnectionIO[IO[Json]] =
      a match {
        case tc @ ToConst(_, _, _, _) => encode(tc)(toConst(tc, st, context))
        case cb @ ToCallback(_, _, _, _) =>
          encode(cb)(toCallback(cb, st, context))
        case ml @ MakeList(_, _, _) => encode(ml)(makeList(ml, st, context))
        case ul @ UnList(_, _, _)   => encode(ul)(unList(ul, st, context))
        case InitTables =>
          encode(InitTables)(st.dbControl.initializeTables.as(IO.unit))
        case CheckTimeouts =>
          encode(CheckTimeouts)(
            st.dbControl.resendNotifications(st.makeAsyncLambda)
          )
        case as @ AllocSlots(count) =>
          encode(as) {
            st.dbControl.allocSlots(count).map(IO.pure)
          }
        case rs @ ReadSlot(slot) =>
          encode(rs) {
            st.dbControl.readSlot(slot).map { resp =>
              IO.pure(PuaAws.Maybe.fromOption(resp))
            }
          }
        case cs @ CompleteSlot(slot, json) =>
          encode(cs) {
            st.dbControl.completeSlot(
              slot,
              PuaAws.Error.NotError(json),
              st.makeAsyncLambda
            )
          }
      }

    val dbOp: ConnectionIO[IO[Json]] =
      LiftIO[ConnectionIO]
        .liftIO(input)
        //.map { act => println(act); act }
        .flatMap(process)

    // we can retry transient operations here:
    def runWithRetries[A](io: IO[A], retry: Int, max: Int): IO[A] =
      io.recoverWith {
        case (_: SQLTransientException | _: PSQLException) if retry < max =>
          // if we have a transient failure, just retry as long
          // as the lambda has a budget for
          val nextR = retry + 1
          nextSleep(nextR) *> runWithRetries(io, nextR, max)
        case err =>
          input.flatMap { act =>
            IO { println("=" * 800); println(s"on: $act\nerror: $err") } *> IO
              .raiseError(err)
          }
      }

    // we arbitrarily set to 50 here.
    val max = 50
    // phase1 is all the db writes without doing any IO
    // after we do that, we do remote IO and maybe some more db writes
    runWithRetries(st.dbControl.run(dbOp).flatten, 0, max)
  }
}
