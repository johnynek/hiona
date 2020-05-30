package dev.posco.hiona.aws

import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, IO, LiftIO, Timer}
import cats.{Applicative, Defer, Monad}
import com.amazonaws.services.lambda.runtime.Context
import doobie.ConnectionIO
import io.circe.{Decoder, Encoder, Json}
import org.postgresql.util.PSQLException
import scala.concurrent.duration.FiniteDuration
import java.sql.SQLTransientException

import cats.implicits._
import doobie.implicits._
import io.circe.syntax._

class PuaAws(
    dbControl: DBControl,
    invokePuaWorkerAsync: Json => IO[Unit]
) extends SlotEnv {
  // some ID in a data-base we can use
  // to find a pointer to result data
  type SlotId = Long

  /**
    * This tracks a value A and the set of opened
    * slots
    */
  type Slots[A] = ConnectionIO[A]
  implicit def applicativeSlots: Applicative[Slots] =
    Monad[ConnectionIO]

  type Eff[A] = IO[A]
  implicit def monadEff: Monad[Eff] =
    cats.effect.Effect[IO]

  def allocSlot = dbControl.allocSlot

  // run the lambda as an event-like function with a given
  // location to put the result
  // this is also responsible for updating the state of the db
  // if the arg slot is not yet ready, we update the list of
  // nodes waiting on the arg
  def toFnLater(ln: LambdaFunctionName, arg: SlotId, out: SlotId): Eff[Unit] = {
    val action: PuaAws.Action = PuaAws.Action.ToCallback(ln, arg, out, None)
    invokePuaWorkerAsync(action.asJson)
  }

  // We have to wait for inputs to be ready for constant
  // functions because we need to order effects correctl;
  def toConst(json: Json, arg: SlotId, out: SlotId): Eff[Unit] = {
    val action: PuaAws.Action = PuaAws.Action.ToConst(json, arg, out, None)
    invokePuaWorkerAsync(action.asJson)
  }

  // this is really a special job just waits for all the inputs
  // and finally writes to an output
  def makeList(inputs: NonEmptyList[SlotId], output: SlotId): Eff[Unit] = {
    val action: PuaAws.Action = PuaAws.Action.MakeList(inputs, output, None)
    invokePuaWorkerAsync(action.asJson)
  }

  // opposite of makeList, wait on a slot, then unlist it
  def unList(input: SlotId, outputs: NonEmptyList[SlotId]): Eff[Unit] = {
    val action: PuaAws.Action = PuaAws.Action.UnList(input, outputs, None)
    invokePuaWorkerAsync(action.asJson)
  }

  def pollSlot[A: Decoder](slotId: Long): IO[Option[A]] =
    dbControl
      .run(dbControl.readSlot(slotId))
      .flatMap {
        case Some(Right(json)) => IO.fromEither(json.as[A]).map(Some(_))
        case Some(Left(err))   => IO.raiseError(err)
        case None              => IO.pure(None)
      }
      .recoverWith {
        case (_: SQLTransientException | _: PSQLException) => IO.pure(None)
      }

  def toIOFn[A: Encoder](
      pua: Pua
  ): A => IO[Long] = {
    val popt = pua.optimize

    val buildSlots =
      for {
        input <- allocSlot
        fn <- start(popt)
      } yield (fn, input)

    { a: A =>
      // no one knows about our slot, so no one is waiting
      // on it, so we don't need to have a real invocation
      val noWaiters: IO[Json] =
        IO.raiseError(new Exception("expected no waiters"))
      val transaction =
        for {
          (fn, slot) <- buildSlots
          _ <-
            dbControl.completeSlot(slot, Right(a.asJson), _ => _ => noWaiters)
        } yield fn(slot)

      lazy val io: IO[Long] = dbControl
        .run(transaction)
        .flatten
        .recoverWith {
          case (_: SQLTransientException | _: PSQLException) => io
        }

      io
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
  sealed abstract class Action

  sealed abstract class BlockingAction extends Action {
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
    case class ToConst(
        json: Json,
        argSlot: Long,
        resultSlot: Long,
        waitId: Option[Long]
    ) extends BlockingAction {
      def argSlots = NonEmptyList(argSlot, Nil)
    }
    case class ToCallback(
        name: LambdaFunctionName,
        argSlot: Long,
        resultSlot: Long,
        waitId: Option[Long]
    ) extends BlockingAction {
      def argSlots = NonEmptyList(argSlot, Nil)
    }
    case class MakeList(
        argSlots: NonEmptyList[Long],
        resultSlot: Long,
        waitId: Option[Long]
    ) extends BlockingAction
    case class UnList(
        argSlot: Long,
        resultSlots: NonEmptyList[Long],
        waitId: Option[Long]
    ) extends BlockingAction {
      def argSlots = NonEmptyList(argSlot, Nil)
    }

    // create tables
    case object InitTables extends Action

    implicit val actionEncoder: Encoder[Action] = {
      import io.circe.generic.semiauto._

      val toConst: Encoder[ToConst] = deriveEncoder[ToConst]
      val toCallback: Encoder[ToCallback] = deriveEncoder[ToCallback]
      val makeList: Encoder[MakeList] = deriveEncoder[MakeList]
      val unList: Encoder[UnList] = deriveEncoder[UnList]

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
          }
      }
    }

    implicit val actionDecoder: Decoder[Action] = {
      import io.circe.{DecodingFailure, HCursor}
      import io.circe.generic.semiauto._

      val toConst: Decoder[ToConst] = deriveDecoder[ToConst]
      val toCallback: Decoder[ToCallback] = deriveDecoder[ToCallback]
      val makeList: Decoder[MakeList] = deriveDecoder[MakeList]
      val unList: Decoder[UnList] = deriveDecoder[UnList]

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

  final case class Error(code: Int, message: String) extends Exception(message)
  object Error {
    val InvocationError: Int = 1
    val InvalidUnlistLength: Int = 2

    def fromInvocationError(err: Throwable): Error =
      Error(InvocationError, err.getMessage())
  }

  case class State(
      dbControl: DBControl,
      makeLambda: LambdaFunctionName => Json => IO[Json],
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

  def allocSlot: ConnectionIO[Long]

  def readSlot(slotId: Long): ConnectionIO[Option[Either[PuaAws.Error, Json]]]

  def addWaiter(
      act: PuaAws.BlockingAction,
      function: LambdaFunctionName
  ): ConnectionIO[Unit]

  def removeWaiter(act: PuaAws.BlockingAction): ConnectionIO[Unit]

  def completeSlot(
      slotId: Long,
      result: Either[PuaAws.Error, Json],
      invoker: LambdaFunctionName => Json => IO[Json]
  ): ConnectionIO[IO[Unit]]
}

class PuaWorker extends PureLambda[PuaAws.State, PuaAws.Action, Unit] {
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
              dbControl.completeSlot(tc.resultSlot, thisRes, st.makeLambda)
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
      val res = json.leftMap(PuaAws.Error.fromInvocationError(_))
      st.dbControl
        .run(for {
          next <- st.dbControl.completeSlot(tc.resultSlot, res, st.makeLambda)
          _ <- st.dbControl.removeWaiter(tc)
        } yield next)
        .flatten
    }

    st.dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(Right(json)) =>
          // the slot is complete, we can now fire off an event:
          val fn = st.makeLambda(tc.name)
          val invoke = fn(json).attempt.flatMap(write)
          Applicative[ConnectionIO].pure(invoke)
        case Some(err @ Left(_)) =>
          st.dbControl.completeSlot(tc.resultSlot, err, st.makeLambda) <*
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
            val write = nelJson.sequence.map { noErrors =>
              Json.fromValues(noErrors.toList)
            }
            dbControl.completeSlot(ml.resultSlot, write, st.makeLambda) <*
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
            case Right(success) =>
              val resSize = ul.resultSlots.size
              success.asArray match {
                case Some(v) if v.size == resSize =>
                  ul.resultSlots.toList.toVector
                    .zip(v)
                    .traverse {
                      case (slot, json) =>
                        dbControl.completeSlot(slot, Right(json), st.makeLambda)
                    }
                    .map(_.sequence_)
                case _ =>
                  val err = PuaAws.Error(
                    PuaAws.Error.InvalidUnlistLength,
                    s"expected a list of size: $resSize, got: ${success.noSpaces}"
                  )

                  ul.resultSlots
                    .traverse { slot =>
                      dbControl.completeSlot(slot, Left(err), st.makeLambda)
                    }
                    .map(_.sequence_)
              }
            case err @ Left(_) =>
              // set all the downstreams as failures
              ul.resultSlots
                .traverse { slot =>
                  dbControl.completeSlot(slot, err, st.makeLambda)
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

  def run(input: IO[PuaAws.Action], st: State, context: Context): IO[Unit] = {
    val rng = new java.util.Random(context.getAwsRequestId().hashCode)
    val nextDouble: IO[Double] = IO(rng.nextDouble)

    def nextSleep(retry: Int): IO[Unit] =
      for {
        d <- nextDouble
        dr = retry * 100.0 * d
        dur = FiniteDuration(dr.toLong, "ms")
        _ <- st.timer.sleep(dur)
      } yield ()

    val dbOp: ConnectionIO[IO[Unit]] =
      LiftIO[ConnectionIO]
        .liftIO(input)
        //.map { act => println(act); act }
        .flatMap {
          case tc @ ToConst(_, _, _, _)    => toConst(tc, st, context)
          case cb @ ToCallback(_, _, _, _) => toCallback(cb, st, context)
          case ml @ MakeList(_, _, _)      => makeList(ml, st, context)
          case ul @ UnList(_, _, _)        => unList(ul, st, context)
          case InitTables                  => st.dbControl.initializeTables.as(IO.unit)
        }

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
    val phase1 = st.dbControl.run(dbOp)
    runWithRetries(phase1, 0, max)
      .flatMap(runWithRetries(_, 0, max))
  }
}
