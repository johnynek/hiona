package dev.posco.hiona.aws

import cats.data.NonEmptyList
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, ContextShift, IO, Resource}
import com.amazonaws.services.lambda.runtime.Context
import doobie._
import doobie.enum.TransactionIsolation.TransactionSerializable
import io.circe.Json
import org.scalacheck.Prop
import scala.concurrent.duration.FiniteDuration

import cats.implicits._
import doobie.implicits._

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
    sem.withPermit(super.run(con))
}

object H2DBControl {
  def apply(
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, H2DBControl] = {
    val xa0 = Transactor.fromDriverManager[IO](
      classOf[org.h2.Driver].getName,
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;LOCK_MODE=1",
      //"jdbc:h2:~/test;LOCK_MODE=1",
      "sa", // user
      "", // password
      blocker
    )

    val b = for {
      _ <- FC.setAutoCommit(false)
      _ <-
        sql"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.void
      _ <- HC.setTransactionIsolation(TransactionSerializable)
    } yield ()

    val xa = Transactor.before.set(xa0, b)

    Resource.liftF(Semaphore[IO](1).map(new H2DBControl(xa, _)))
  }
}

class PuaAwsTest extends munit.ScalaCheckSuite {
  implicit val ctx = IO.contextShift(scala.concurrent.ExecutionContext.global)

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
        Blocker[IO].flatMap(b => H2DBControl(b).map((b, _))).use {
          case (blocker, dbControl) =>
            // make the rng deterministic
            val rng = new java.util.Random(123)
            // assign a random name for the lambda
            val lambdaName: String =
              Iterator
                .continually(rng.nextInt.toString)
                .dropWhile(nm => dir0.contains(LambdaFunctionName(nm)))
                .next

            // this is yucky, but we have a circular dependency
            // that is broken before we invoke
            val workerRef: Ref[IO, Json => IO[Unit]] =
              Ref.unsafe(_ => IO.unit)

            val invokeLam: Json => IO[Unit] = { j: Json =>
              IO.suspend {
                workerRef.get.flatMap(_(j))
              }
            }

            val dir = dir0.addFn(lambdaName, invokeLam)

            val setupH2Worker: PuaAws.State =
              PuaAws.State(dbControl, dir, blocker, ctx)

            val worker = new PuaWorker {
              override def setup =
                IO.pure(setupH2Worker)
                  .as(setupH2Worker)
            }

            val puaAws = new PuaAws(dbControl, invokeLam)

            implicit val timer =
              IO.timer(scala.concurrent.ExecutionContext.global)

            val mockContext: Context =
              new Context {
                def getAwsRequestId(): String = ???
                def getClientContext()
                    : com.amazonaws.services.lambda.runtime.ClientContext = ???
                def getFunctionName(): String = ???
                def getFunctionVersion(): String = ???
                def getIdentity()
                    : com.amazonaws.services.lambda.runtime.CognitoIdentity =
                  ???
                def getInvokedFunctionArn(): String = lambdaName
                def getLogGroupName(): String = ???
                def getLogStreamName(): String = ???
                def getLogger()
                    : com.amazonaws.services.lambda.runtime.LambdaLogger = ???
                def getMemoryLimitInMB(): Int = ???
                def getRemainingTimeInMillis(): Int = ???
              }

            val setWorker = workerRef.set { j =>
              worker
                .run(
                  IO.fromEither(j.as[PuaAws.Action]),
                  setupH2Worker,
                  mockContext
                )
                .start
                .void
            /*
             the tests fail to complete with the following
             that means this state machine is not yet safe
             with races. We probably need to have a polling
             mechanism to wake up and try again if messages
             are received out of order, or we need to be
             more careful in the state transitions
             */
            }

            val dbFn =
              puaAws.toIOFnPoll[Json, Json](pua, FiniteDuration(50, "ms"))

            for {
              _ <- dbControl.run(dbControl.initializeTables).attempt
              _ <- setWorker
              res <- inputs.parTraverse(dbFn)
              _ <- dbControl.run(dbControl.cleanupTables)
            } yield res
        }

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
        .unsafeRunSync()
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
