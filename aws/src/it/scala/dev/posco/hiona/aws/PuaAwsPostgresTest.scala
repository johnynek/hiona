package dev.posco.hiona.aws

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, IO}
import com.amazonaws.services.lambda.runtime.Context
import io.circe.Json
import org.scalacheck.Prop
import scala.concurrent.duration.FiniteDuration

import cats.implicits._

class PuaAwsPostgresTest extends munit.ScalaCheckSuite {
  implicit val ctx = IO.contextShift(scala.concurrent.ExecutionContext.global)
  val timer = IO.timer(scala.concurrent.ExecutionContext.global)

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        20
      ) // a bit slow, but locally, this passes with more

  //override val scalaCheckInitialSeed = "gV_zHGkPkmQY_dODNFYtlHXPSnq6eea0NHyGt-9VNpK="

  property("postgres puaaws matches what we expect") {

    def law(
        pua: Pua,
        dir0: PuaLocal.Directory,
        inputs: List[Json],
        fn: Json => IO[Json]
    ) = {
      val computation =
        Blocker[IO]
        .flatMap { blocker =>
          PostgresDBControl("sbt_it_tests", "sbtester", "sbtpw", blocker)
            .map((blocker, _))
        }
        .use { case (blocker, dbControl) =>
            // make the rng deterministic
            val rng = new java.util.Random(123)
            // assign a random name for the lambda
            val lambdaName: String =
              Iterator
                .continually(rng.nextInt.toString)
                .dropWhile(nm => dir0.contains(LambdaFunctionName(nm)))
                .next

            val usedRequests = scala.collection.mutable.Set[String]()
            def nextReq: String = {
              usedRequests.synchronized {
                val reqId = rng.nextInt.toString
                if (usedRequests(reqId)) nextReq
                else {
                  usedRequests += reqId
                  reqId
                }
              }
            }

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
            val dirA = { ln: LambdaFunctionName => { json: Json => dir(ln)(json).start.void } }

            implicit val timer =
              IO.timer(scala.concurrent.ExecutionContext.global)

            val setupPostgresWorker: PuaAws.State =
              PuaAws.State(dbControl, dir, dirA, blocker, ctx, timer)

            val worker = new PuaWorker {
              override def setup =
                IO.pure(setupPostgresWorker)
                  .as(setupPostgresWorker)
            }

            val puaAws = new PuaAws(dbControl, invokeLam)

            def mockContext: Context =
              new Context {
                lazy val getAwsRequestId: String = nextReq
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
                  setupPostgresWorker,
                  mockContext
                )
                .start
                .void
            }

            val dbFn =
              puaAws.toIOFnPoll[Json, Json](pua, FiniteDuration(50, "ms"))

            for {
              _ <- dbControl.run(dbControl.initializeTables)
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

    Prop.forAllNoShrink(PuaGens.genDirectory) {
      case (pua, dir0, inputs, fn) => law(pua, dir0, inputs, fn)
    }
  }
}
