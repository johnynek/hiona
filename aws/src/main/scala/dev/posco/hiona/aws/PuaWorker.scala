package dev.posco.hiona.aws

import cats.Applicative
import cats.effect.{IO, Timer}
import com.amazonaws.services.lambda.runtime.Context
import doobie.ConnectionIO
import io.circe.{Encoder, Json}
import org.postgresql.util.PSQLException
import java.sql.SQLTransientException
import scala.concurrent.duration.FiniteDuration

import io.circe.syntax._
import cats.implicits._

import PuaAws.Call
import PuaAws.Or
import PuaAws.Action._
import PuaAws.Call.{empty => emptyCall, notifications}
import PuaWorker.State

/**
  * This represents the Lambda that manages the DB state for Pua
  */
abstract class PuaWorker
    extends PureLambda[PuaWorker.State, PuaAws.Action, Json] {

  def toConst(
      tc: ToConst,
      st: State,
      context: Context
  ): ConnectionIO[Call] = {
    import st.dbControl

    dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(either) =>
          // data or error exists, so, we can write the data
          val thisRes = either.map(_ => tc.json)
          for {
            calls <- dbControl.completeSlot(tc.resultSlot, thisRes)
            _ <- dbControl.removeWaiter(tc)
          } yield notifications(calls)
        case None =>
          //println(s"$tc could not read")
          // we have to wait and callback.
          dbControl
            .addWaiter(
              tc,
              LambdaFunctionName(context.getInvokedFunctionArn())
            )
            .as(emptyCall)
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
  ): ConnectionIO[Call] =
    st.dbControl
      .readSlot(tc.argSlot)
      .flatMap {
        case Some(Or.First(json)) =>
          // the slot is complete, we can now fire off an event:
          Applicative[ConnectionIO].pure(
            Call.Invocation(tc.name, json, tc.resultSlot)
          )
        case Some(err) =>
          for {
            calls <- st.dbControl.completeSlot(tc.resultSlot, err)
            _ <- st.dbControl.removeWaiter(tc)
          } yield notifications(calls)
        case None =>
          //println(s"$tc could not read")
          // we have to wait and callback.
          st.dbControl
            .addWaiter(tc, LambdaFunctionName(context.getInvokedFunctionArn()))
            .as(emptyCall)
      }

  def makeList(
      ml: MakeList,
      st: State,
      context: Context
  ): ConnectionIO[Call] = {
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
            for {
              calls <- dbControl.completeSlot(
                ml.resultSlot,
                PuaAws.Error.fromEither(write)
              )
              _ <- dbControl.removeWaiter(ml)
            } yield notifications(calls)

          case None =>
            // we are not done, continue to wait
            dbControl
              .addWaiter(
                ml,
                LambdaFunctionName(context.getInvokedFunctionArn())
              )
              .as(emptyCall)
        }
      }
  }

  def unList(
      ul: UnList,
      st: State,
      context: Context
  ): ConnectionIO[Call] = {
    import st.dbControl

    dbControl
      .readSlot(ul.argSlot)
      .flatMap {
        case Some(done) =>
          val write = done match {
            case Or.First(success) =>
              val resSize = ul.resultSlots.size
              success.asArray match {
                case Some(v) if v.size == resSize =>
                  ul.resultSlots.toList
                    .zip(v.toList)
                    .traverse {
                      case (slot, json) =>
                        dbControl.completeSlot(
                          slot,
                          Or.First(json)
                        )
                    }
                    .map(cs => notifications(cs.flatten))
                case _ =>
                  val err = Or.Second(
                    PuaAws.Error(
                      PuaAws.Error.InvalidUnlistLength,
                      s"expected a list of size: $resSize, got: ${success.noSpaces}"
                    )
                  )

                  ul.resultSlots
                    .traverse { slot =>
                      dbControl.completeSlot(
                        slot,
                        err
                      )
                    }
                    .map(cs => notifications(cs.toList.flatten))
              }
            case err => // this has to be Or.Second
              // set all the downstreams as failures
              ul.resultSlots
                .traverse { slot =>
                  dbControl.completeSlot(slot, err)
                }
                .map(cs => notifications(cs.toList.flatten))
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
            .as(emptyCall)
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
        resp: ConnectionIO[a.Resp]
    )(implicit enc: Encoder[a.Resp]): ConnectionIO[Json] =
      resp.map(_.asJson)

    def process(a: PuaAws.Action): ConnectionIO[Json] =
      a match {
        case tc @ ToConst(_, _, _, _) =>
          encode(tc)(toConst(tc, st, context))
        case cb @ ToCallback(_, _, _, _) =>
          encode(cb)(toCallback(cb, st, context))
        case ml @ MakeList(_, _, _) =>
          encode(ml)(makeList(ml, st, context))
        case ul @ UnList(_, _, _) =>
          encode(ul)(unList(ul, st, context))
        case InitTables =>
          encode(InitTables)(st.dbControl.initializeTables)
        case CheckTimeouts =>
          encode(CheckTimeouts)(
            st.dbControl.resendNotifications.map(notifications(_))
          )
        case as @ AllocSlots(count) =>
          encode(as) {
            st.dbControl.allocSlots(count)
          }
        case rs @ ReadSlot(slot) =>
          encode(rs) {
            st.dbControl.readSlot(slot).map(PuaAws.Maybe.fromOption)
          }
        case cs @ CompleteSlot(slot, json) =>
          encode(cs) {
            st.dbControl
              .completeSlot(
                slot,
                json
              )
              .map(notifications)
          }
      }

    // we can retry transient operations here:
    def runWithRetries[A](io: IO[A], retry: Int, max: Int): IO[A] =
      io.recoverWith {
        case ex @ (_: SQLTransientException | _: PSQLException)
            if retry < max =>
          // if we have a transient failure, just retry as long
          // as the lambda has a budget for
          val nextR = retry + 1
          for {
            _ <- log(context, s"retry = $retry\n$ex")
            _ <- nextSleep(nextR)
            a <- runWithRetries(io, nextR, max)
          } yield a
      }

    // we arbitrarily set to 50 here.
    val max = 50
    // phase1 is all the db writes without doing any IO
    // after we do that, we do remote IO and maybe some more db writes
    input.flatMap { action =>
      val dbOp = process(action)

      val logged =
        for {
          res <- st.dbControl.run(dbOp)
          _ <- log(context, s"result: ${res.noSpaces}")
        } yield res

      log(context, s"called with: ${action.asJson.noSpaces}") *>
        runWithRetries(logged, 0, max)
    }
  }
}

object PuaWorker {
  final case class State(
      dbControl: DBControl,
      timer: Timer[IO]
  )
}

/**
  * This lives outside the VPC where it can easily reach
  * the AWS Lambda service and invoke any other function
  * This does not need any resource behind the VPC, all
  * such resources should be served by lambdas this calls
  */
abstract class PuaCaller
    extends PureLambda[
      PuaCaller.State,
      Or[PuaAws.Action.CheckTimeouts.type, PuaAws.Call],
      Unit
    ] {

  def runCall(
      call: PuaAws.Call,
      state: PuaCaller.State,
      context: Context
  ): IO[Unit] =
    call match {
      case Call.Notification(fn, arg) =>
        val sync = state.makeLambda

        def maybeCall(j: Json): Option[PuaAws.Call] =
          j.as[PuaAws.Call].fold(_ => None, Some(_))

        for {
          _ <- log(context, s"about to notify: ${fn.asString}(${arg.noSpaces})")
          respJson <- sync(fn)(arg)
          resp = maybeCall(respJson)
          _ <- resp.fold(log(context, s"expected: $respJson to be a Call"))(
            runCall(_, state, context)
          )
        } yield ()
      case Call.Invocation(fn, arg, resultSlot) =>
        val sync = state.makeLambda

        for {
          _ <- log(context, s"about to call: ${fn.asString}(${arg.noSpaces})")
          jsone <- sync(fn)(arg).attempt
          _ <-
            log(context, s"result of call to ${fn.asString}(${arg.noSpaces})")
          res = PuaAws.Error.fromEither(jsone)
          complete = PuaAws.Action.CompleteSlot(resultSlot, res)
          nextCall <- state.callDB(complete)
          _ <- runCall(nextCall, state, context)
        } yield ()
      case Call.Repeated(items) =>
        items.traverse_(runCall(_, state, context))
    }

  def run(
      in: IO[Or[PuaAws.Action.CheckTimeouts.type, PuaAws.Call]],
      state: PuaCaller.State,
      context: Context
  ): IO[Unit] =
    in.flatMap {
      case Or.First(call) => runCall(call, state, context)
      case Or.Second(checktime) =>
        state.callDB(checktime).flatMap(runCall(_, state, context))
    }
}

object PuaCaller {
  final case class State(
      puaDBLambda: LambdaFunctionName,
      makeLambda: LambdaFunctionName => Json => IO[Json],
      makeAsyncLambda: LambdaFunctionName => Json => IO[Unit]
  ) {
    def callDB(act: PuaAws.Action): IO[PuaAws.Call] =
      for {
        callJson <- makeLambda(puaDBLambda)(act.asJson)
        call <- IO.fromEither(callJson.as[PuaAws.Call])
      } yield call
  }
}
