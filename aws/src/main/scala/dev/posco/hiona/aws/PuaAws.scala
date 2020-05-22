package dev.posco.hiona.aws

import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, IO, LiftIO}
import cats.{Applicative, Monad}
import com.amazonaws.services.lambda.runtime.Context
import doobie.{ConnectionIO, HC, Transactor}
import doobie.enum.TransactionIsolation.TransactionSerializable
import io.circe.{Decoder, Encoder, Json}

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
      .readSlot(slotId)
      .transact(dbControl.transactor)
      .flatMap {
        case Some(Right(json)) => IO.fromEither(json.as[A]).map(Some(_))
        case Some(Left(err))   => IO.raiseError(err)
        case None              => IO.pure(None)
      }

  def toIOFn[A: Encoder](
      pua: Pua
  ): A => IO[Long] = {
    val popt = pua.optimize

    val buildSlots =
      for {
        _ <- HC.setTransactionIsolation(TransactionSerializable)
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

      for {
        io <- transaction.transact(dbControl.transactor)
        res <- io
      } yield res
    }
  }
}

object PuaAws {
  sealed abstract class Action
  object Action {
    case class ToConst(
        json: Json,
        argSlot: Long,
        resultSlot: Long,
        waitId: Option[Long]
    ) extends Action
    case class ToCallback(
        name: LambdaFunctionName,
        argSlot: Long,
        resultSlot: Long,
        waitId: Option[Long]
    ) extends Action
    case class MakeList(
        argSlots: NonEmptyList[Long],
        resultSlot: Long,
        waitId: Option[Long]
    ) extends Action
    case class UnList(
        argSlot: Long,
        resultSlots: NonEmptyList[Long],
        waitId: Option[Long]
    ) extends Action

    implicit val actionEncoder: Encoder[Action] = ???
    implicit val actionDecoder: Decoder[Action] = ???
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
      ctxShift: ContextShift[IO]
  ) {

    def transactor: Transactor[IO] = dbControl.transactor
  }

  def buildLambdaState: IO[State] = ???
}

/**
  * Data structures:
  * Waiter: FnName, Json argument to send
  *
 * key: slot: Long, update: Timestamp, result: Option[Json], errNumber: Option[Int], failure: Option[String], waiters: Option[List[Waiter]]
  case class Slot(
      slotId: Long,
      update: SQLTimestamp,
      result: Option[String],
      errorCode: Option[Int],
      errorMessage: Option[String],
      waiters: List[Long]
  )
  case class Waiter(
      waiterId: Long,
      update: SQLTimestamp,
      functionName: String,
  )
  */
abstract class DBControl {

  def transactor: Transactor[IO]

  def initializeTables: ConnectionIO[Unit]

  def allocSlot: ConnectionIO[Long]

  def readSlot(slotId: Long): ConnectionIO[Option[Either[PuaAws.Error, Json]]]

  def addWaiter(
      act: PuaAws.Action,
      function: LambdaFunctionName
  ): ConnectionIO[Unit]

  def removeWaiter(act: PuaAws.Action): ConnectionIO[Unit]

  def completeSlot(
      slotId: Long,
      result: Either[PuaAws.Error, Json],
      invoker: LambdaFunctionName => Json => IO[Json]
  ): ConnectionIO[Unit]
}

class PuaWorker extends PureLambda[PuaAws.State, PuaAws.Action, Unit] {
  import PuaAws.Action._
  import PuaAws.State

  def setup = PuaAws.buildLambdaState

  def toConst(tc: ToConst, st: State, context: Context): ConnectionIO[Unit] = {
    import st.dbControl

    dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(either) =>
          // data or error exists, so, we can write the data
          val thisRes = either.map(_ => tc.json)
          for {
            _ <- dbControl.completeSlot(tc.resultSlot, thisRes, st.makeLambda)
            _ <- dbControl.removeWaiter(tc)
          } yield ()
        case None =>
          // we have to wait and callback.
          dbControl.addWaiter(
            tc,
            LambdaFunctionName(context.getInvokedFunctionArn())
          )
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
  ): IO[Unit] = {
    // do the finishing transaction, including removing the waiter
    def write(json: Either[Throwable, Json]): IO[Unit] = {
      val res = json.leftMap(PuaAws.Error.fromInvocationError(_))
      (st.dbControl.completeSlot(tc.resultSlot, res, st.makeLambda) *>
        st.dbControl.removeWaiter(tc)).transact(st.transactor)
    }

    val toIO: ConnectionIO[IO[Unit]] = st.dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(Right(json)) =>
          // the slot is complete, we can now fire off an event:
          val fn = st.makeLambda(tc.name)
          Applicative[ConnectionIO].pure(
            for {
              jresEither <- fn(json).attempt
              _ <- write(jresEither)
            } yield ()
          )
        case Some(err @ Left(_)) =>
          st.dbControl.completeSlot(tc.resultSlot, err, st.makeLambda) *>
            st.dbControl.removeWaiter(tc).as(IO.unit)
        case None =>
          // we have to wait and callback.
          st.dbControl
            .addWaiter(tc, LambdaFunctionName(context.getInvokedFunctionArn()))
            .as(IO.unit)
      }

    // we exit the transaction for the inner action
    toIO.transact(st.transactor).flatten
  }

  def makeList(
      ml: MakeList,
      st: State,
      context: Context
  ): ConnectionIO[Unit] = {
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
            dbControl.completeSlot(ml.resultSlot, write, st.makeLambda) *>
              dbControl.removeWaiter(ml)

          case None =>
            // we are not done, continue to wait
            dbControl.addWaiter(
              ml,
              LambdaFunctionName(context.getInvokedFunctionArn())
            )
        }
      }
  }

  def unList(ul: UnList, st: State, context: Context): ConnectionIO[Unit] = {
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
                    .traverse_ {
                      case (slot, json) =>
                        dbControl.completeSlot(slot, Right(json), st.makeLambda)
                    }
                case _ =>
                  val err = PuaAws.Error(
                    PuaAws.Error.InvalidUnlistLength,
                    s"expected a list of size: $resSize, got: ${success.noSpaces}"
                  )

                  ul.resultSlots.traverse_ { slot =>
                    dbControl.completeSlot(slot, Left(err), st.makeLambda)
                  }
              }
            case err @ Left(_) =>
              // set all the downstreams as failures
              ul.resultSlots.traverse_ { slot =>
                dbControl.completeSlot(slot, err, st.makeLambda)
              }
          }

          write *> dbControl.removeWaiter(ul)
        case None =>
          // we are not done, continue to wait
          dbControl.addWaiter(
            ul,
            LambdaFunctionName(context.getInvokedFunctionArn())
          )
      }
  }

  def run(input: IO[PuaAws.Action], st: State, context: Context): IO[Unit] = {
    val dbOp = LiftIO[ConnectionIO].liftIO(input).flatMap {
      case tc @ ToConst(_, _, _, _) => toConst(tc, st, context)
      case cb @ ToCallback(_, _, _, _) =>
        LiftIO[ConnectionIO].liftIO(toCallback(cb, st, context))
      case ml @ MakeList(_, _, _) => makeList(ml, st, context)
      case ul @ UnList(_, _, _)   => unList(ul, st, context)
    }

    // we can retry transient operations here:
    dbOp.transact(st.transactor)
  }
}
