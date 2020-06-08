package dev.posco.hiona.aws

import cats.effect.{Blocker, IO}
import io.circe.Json
import org.scalacheck.Prop

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
        }
        .use(H2DBControl.makeComputation(_, pua, dir0, inputs))

      H2DBControl.await30 {
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
      }
    }

    Prop.forAllNoShrink(PuaGens.genDirectory) {
      case (pua, dir0, inputs, fn) => law(pua, dir0, inputs, fn)
    }
  }
}
