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

import cats.Defer
import cats.data.NonEmptyList
import cats.effect.std.Semaphore
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.effect.{Ref, Temporal}
import com.amazonaws.services.lambda.runtime.Context
import doobie._
import doobie.enumerated.TransactionIsolation.TransactionSerializable
import io.circe.Json
import org.scalacheck.Prop
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import cats.implicits._
import doobie.implicits._

import PuaAws.Or
/*
object ImpLog {
  implicit val han = doobie.util.log.LogHandler.jdkLogHandler
}

import ImpLog.han
 */

class H2DBControl(transactor: Transactor[IO], sem: Semaphore[IO])
    extends PostgresDBControl(transactor) {
  override def run[A](con: ConnectionIO[A]): IO[A] =
    // H2 seems to have bugs with serializability
    for {
      _ <- sem.acquire
      a <- super.run(con)
      _ <- sem.release
    } yield a
}

object H2DBControl {
  def apply(): Resource[IO, H2DBControl] = {
    val xa0 = Transactor.fromDriverManager[IO](
      classOf[org.h2.Driver].getName,
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;LOCK_MODE=1",
      // "jdbc:h2:~/test;LOCK_MODE=1",
      "sa", // user
      "" // password
    )

    val b = for {
      _ <- FC.setAutoCommit(false)
      _ <-
        sql"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.void
      _ <- HC.setTransactionIsolation(TransactionSerializable)
    } yield ()

    val xa = Transactor.before.set(xa0, b)

    Resource.eval(Semaphore[IO](1).map(new H2DBControl(xa, _)))
  }

  def resender[A](
      stop: IO[Boolean],
      dur: FiniteDuration,
      action: IO[Unit]
  )(implicit t: Temporal[IO]): IO[Unit] =
    Defer[IO]
      .fix[Unit] { recur =>
        for {
          _ <- t.sleep(dur)
          stopV <- stop
          _ <- if (stopV) IO.unit else (action.start *> recur)
        } yield ()
      }
      .start
      .void

  def makeComputation(
      dbControl: DBControl,
      pua: Pua,
      dir0: PuaLocal.Directory,
      inputs: List[Json]
  ): IO[List[Json]] = {
    // make the rng deterministic
    val rng = new java.util.Random(123)
    // assign a random name for the lambda
    def lambdaName(): String =
      Iterator
        .continually(rng.nextInt.toString)
        .dropWhile(nm => dir0.contains(LambdaFunctionName(nm)))
        .next()

    val dbLam = lambdaName()
    val callerLam = lambdaName()

    val usedRequests = scala.collection.mutable.Set[String]()
    def nextReq: String =
      usedRequests.synchronized {
        val reqId = rng.nextInt.toString
        if (usedRequests(reqId)) nextReq
        else {
          usedRequests += reqId
          reqId
        }
      }

    // this is yucky, but we have a circular dependency
    // that is broken before we invoke
    val workerRef: Ref[IO, Json => IO[Json]] =
      Ref.unsafe(_ => IO.raiseError(new Exception("unitialized")))

    val callerRef: Ref[IO, Json => IO[Unit]] =
      Ref.unsafe(_ => IO.raiseError(new Exception("unitialized")))

    val invokeWorker: Json => IO[Json] = { j: Json =>
      IO.defer {
        workerRef.get.flatMap(_(j))
      }
    }

    val invokeCaller: Json => IO[Unit] = { j: Json =>
      IO.defer {
        callerRef.get.flatMap(_(j))
      }
    }

    val dir1 = dir0
      .addFn(dbLam, invokeWorker)
      .addFn(callerLam, invokeCaller)

    val dir = { ln: LambdaFunctionName => json: Json =>
      dir1(ln)(json).start.void
    }

    val callerState: PuaCaller.State =
      PuaCaller.State(LambdaFunctionName(dbLam), dir1, dir)

    val caller = new PuaCaller {
      override def setup = IO.pure(callerState)
    }

    val setupH2Worker: PuaWorker.State =
      PuaWorker.State(dbControl)

    val worker = new PuaWorker {
      override def setup = IO.pure(setupH2Worker)
    }

    def mockContext(nm: String): Context =
      new Context {
        lazy val getAwsRequestId: String = nextReq
        def getClientContext()
            : com.amazonaws.services.lambda.runtime.ClientContext = ???
        def getFunctionName(): String = ???
        def getFunctionVersion(): String = ???
        def getIdentity()
            : com.amazonaws.services.lambda.runtime.CognitoIdentity =
          ???
        def getInvokedFunctionArn(): String = nm
        def getLogGroupName(): String = ???
        def getLogStreamName(): String = ???
        def getLogger(): com.amazonaws.services.lambda.runtime.LambdaLogger =
          new com.amazonaws.services.lambda.runtime.LambdaLogger {
            def log(str: Array[Byte]): Unit =
              () // println(new String(str, "UTF-8"))
            def log(str: String): Unit = () // println(str)
          }
        def getMemoryLimitInMB(): Int = ???
        def getRemainingTimeInMillis(): Int = ???
      }

    val syncFn: Json => IO[Json] = { j: Json =>
      IO.fromEither(j.as[PuaAws.Action])
        .flatMap(worker.run(_, setupH2Worker, mockContext(dbLam)))
    }

    /*
    val lossyRng = new java.util.Random(42)

    def makeLossy[A](fn: Json => IO[A]): Json => IO[Unit] = { j: Json =>
      // if we lose initial messages adding waiters
      // it still deadlocks
      IO.suspend {
        j.as[PuaAws.Action] match {
          case Right(ba: PuaAws.BlockingAction)
              if ba.waitId.isDefined =>
            if (lossyRng.nextDouble() < 0.2)
              // 20%
              IO.unit
            else fn(j).start.void
          case _ => fn(j).start.void
        }
      }
    }
    val asyncFn = makeLossy(syncFn)
     */

    val invoke = PuaAws.Invoke.fromSyncAsyncNames(
      dir1,
      dir,
      LambdaFunctionName(dbLam),
      LambdaFunctionName(callerLam)
    )
    val puaAws = new PuaAws(invoke)

    val setWorker = workerRef.set(syncFn)
    val setCaller = callerRef.set { j: Json =>
      IO.fromEither(
        j.as[Or[PuaAws.Action.CheckTimeouts.type, PuaAws.Call]]
      ).flatMap(
        caller
          .run(_, callerState, mockContext(callerLam))
      )
    }

    val doneRef = Ref.unsafe[IO, Boolean](false)
    /*
    val check = resender(
      doneRef.get,
      FiniteDuration(1, "s"),
      caller.run(IO.pure(Or.Second(PuaAws.Action.CheckTimeouts)), callerState, mockContext(callerLam))
    )
    // only needed if messages can be lost, but we aren't simulating that at the moment
     */
    val check = IO.unit

    val dbFn =
      puaAws.toIOFnPoll[Json, Json](pua, FiniteDuration(50, "ms"))

    for {
      _ <- dbControl.run(dbControl.initializeTables).attempt
      _ <- setWorker
      _ <- setCaller
      _ <- check
      res <- inputs.parTraverse(dbFn)
      _ <- doneRef.set(true)
      _ <- dbControl.run(dbControl.cleanupTables)
    } yield res
  }

  def await30[A](io: IO[A]): A =
    Await.result(io.unsafeToFuture()(IORuntime.global), FiniteDuration(30, "s"))
}

class PuaAwsTest extends munit.ScalaCheckSuite {
  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        10
      ) // a bit slow, but locally, this passes with more

  property("h2 puaaws matches what we expect") {

    def law(
        pua: Pua,
        dir0: PuaLocal.Directory,
        inputs: List[Json],
        fn: Json => IO[Json]
    ) = {
      val computation =
        H2DBControl().use(H2DBControl.makeComputation(_, pua, dir0, inputs))

      H2DBControl.await30(
        (computation.attempt, inputs.traverse(fn).attempt)
          .mapN {
            case (Right(a), Right(b)) =>
              assertEquals(a, b)
            case (Left(_), Left(_)) =>
              assert(true)
            case (left, right) =>
              // this will fail, but we want to see what we get
              left match {
                case Left(err) => err.printStackTrace
                case Right(_)  => ()
              }
              assertEquals(left, right)
          }
      )
    }

    val dir = PuaLocal.emptyDirectory
      .addFn("a", { j: Long => IO.pure(j) })
      .addFn("b", { j: Long => IO.pure(j + 1) })
      .addFn("c", { j: Json => IO.pure(j) })
      .addFn("d", { j: Json => IO.pure(j) })

    val regression0 =
      Pua.Compose(
        Pua.Fanout(NonEmptyList.of(Pua.call("a"), Pua.call("b"))),
        Pua.Compose(Pua.call("c"), Pua.call("d"))
      )

    law(
      regression0,
      dir,
      List(Json.fromLong(1L)),
      { j: Json =>
        val j1 = Json.fromLong(
          j.as[Long].getOrElse(sys.error(s"expected long in $j")) + 1L
        )
        IO.pure(Json.fromValues(List(j, j1)))
      }
    )

    Prop.forAllNoShrink(PuaGens.genDirectory) {
      case (pua, dir0, inputs, fn) => law(pua, dir0, inputs, fn)
    }
  }
}
