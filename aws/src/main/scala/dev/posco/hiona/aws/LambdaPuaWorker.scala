package dev.posco.hiona.aws

import LambdaDeploy.LambdaMethods
import RDSTransactor.DatabaseName
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import com.amazonaws.services.lambda.AWSLambda
import com.amazonaws.services.lambda.model.Runtime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.s3
import com.monovore.decline.{Command, Opts}
import io.circe.Json
import scala.concurrent.duration.FiniteDuration

import scala.jdk.CollectionConverters._
import cats.implicits._

final class LambdaPuaWorker extends PuaWorker {
  def setup =
    for {
      (db, region, sec) <-
        LambdaPuaWorkerState.readEnvArgs(LambdaPuaWorkerState.dbEnvCmd)
      // we are ignoring the shutdown code, since the lambda gives no hooks for this
      (state, _) <- LambdaPuaWorkerState.state(db, region, sec).allocated
    } yield state
}

final class LambdaPuaCaller extends PuaCaller {
  def setup =
    for {
      dbFnName <-
        LambdaPuaWorkerState.readEnvArgs(LambdaPuaWorkerState.callerEnvCmd)
      // we are ignoring the shutdown code, since the lambda gives no hooks for this
      (state, _) <- LambdaPuaWorkerState.callerState(dbFnName).allocated
    } yield state
}

object LambdaPuaWorkerState {
  private[this] val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  val defaultDBLambdaName: LambdaFunctionName =
    LambdaFunctionName("hiona_LambdaPuaWorker")

  val defaultCallLambdaName: LambdaFunctionName =
    LambdaFunctionName("hiona_LambdaPuaCaller")

  def makeArgs(
      db: DatabaseName,
      region: String,
      secret: String
  ): Map[String, String] =
    Map(
      "pua_db_name" -> db.asString,
      "pua_db_region" -> region,
      "pua_db_secret" -> secret
    )

  val ioEnv: IO[Map[String, String]] = IO(System.getenv().asScala.toMap)

  val dbEnvCmd: Command[(DatabaseName, String, String)] =
    Command("lambda_pua_worker", "the lambda that manages pua state") {
      (
        Opts.env[String]("pua_db_name", "the name of the db to connect to"),
        Opts
          .env[String]("pua_db_region", "the region for our secret manager"),
        Opts.env[String](
          "pua_db_secret",
          "the secret name for the DB auth info"
        )
      ).mapN { (nm, region, sec) =>
        (DatabaseName(nm), region, sec)
      }
    }

  def makeCallerArgs(lfn: LambdaFunctionName): Map[String, String] =
    Map("pua_db_lambda_name" -> lfn.asString)

  val callerEnvCmd: Command[LambdaFunctionName] =
    Command("lambda_pua_caller", "the lambda that makes DB calls for Pua") {
      Opts
        .env[String](
          "pua_db_lambda_name",
          "the name of the lambda that can talk to the DB"
        )
        .map(LambdaFunctionName(_))
    }

  def readEnvArgs[T](cmd: Command[T]): IO[T] = {

    def result[A, B](e: Either[A, B]): IO[B] =
      e match {
        case Right(b) => IO.pure(b)
        case Left(err) =>
          IO.suspend {
            System.err.println(err.toString)
            IO.raiseError(new Exception(s"could not parse args: $err"))
          }
      }

    for {
      env <- ioEnv
      either = cmd.parse(Nil, env)
      r <- result(either)
    } yield r
  }

  def makeFunctions(awsLambda: AWSLambda, blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): (
      LambdaFunctionName => Json => IO[Json],
      LambdaFunctionName => Json => IO[Unit]
  ) = {
    val syncFn = { nm: LambdaFunctionName => awsLambda.makeLambda(nm, blocker) }
    val asyncFn = { nm: LambdaFunctionName =>
      awsLambda.makeLambdaAsync(nm, blocker)
    }

    (syncFn, asyncFn)
  }

  def state(
      puaDB: DatabaseName,
      region: String,
      dbSecret: String
  ): Resource[IO, PuaWorker.State] = {

    val ec = scala.concurrent.ExecutionContext.global
    implicit val ctx = IO.contextShift(ec)
    val timer = IO.timer(ec)
    for {
      blocker <- Blocker[IO]
      trans <- Resource.liftF(
        RDSTransactor
          .build[IO](region, dbSecret, puaDB, blocker)
      )
      pg = new PostgresDBControl(trans)
    } yield PuaWorker.State(pg, timer)
  }

  def callerState(dbFn: LambdaFunctionName): Resource[IO, PuaCaller.State] = {
    val ec = scala.concurrent.ExecutionContext.global
    implicit val ctx = IO.contextShift(ec)
    for {
      blocker <- Blocker[IO]
      awsLambda <- LambdaDeploy.awsLambda
      (sync, async) = makeFunctions(awsLambda, blocker)
    } yield PuaCaller.State(dbFn, sync, async)
  }

  def puaAws(
      puaDB: LambdaFunctionName,
      puaCaller: LambdaFunctionName,
      aws: AWSLambda,
      blocker: Blocker
  )(implicit
      ctx: ContextShift[IO]
  ): PuaAws = {
    val (sync, async) = makeFunctions(aws, blocker)
    new PuaAws(PuaAws.Invoke.fromSyncAsyncNames(sync, async, puaDB, puaCaller))
  }

  def command(aws: AWSLambda, awsS3: s3.AmazonS3, blocker: Blocker)(implicit
      ctx: ContextShift[IO],
      timer: Timer[IO]
  ): Command[IO[Unit]] = {
    val puaOpt: Opts[IO[Pua]] =
      LambdaDeploy.Payload
        .optPayload("pua_json")
        .map { payload =>
          for {
            json <- payload.toJson
            pex <- IO.fromEither(json.as[Pua.Expr.PuaExpr])
            puaE = Pua.Expr.toPua(pex).toEither
            pua <- puaE match {
              case Right(p) => IO.pure(p)
              case Left(errs) =>
                IO.raiseError(
                  new Exception(
                    s"could not convert pua expression to pua: $errs"
                  )
                )
            }
          } yield pua
        }

    val arg: Opts[IO[Json]] =
      LambdaDeploy.Payload
        .optPayload("arg_json")
        .map(_.toJson)

    val puaDBName: Opts[LambdaFunctionName] =
      Opts
        .option[String]("pua_db_lambda_name", "the name of the pua DB Lambda")
        .map(LambdaFunctionName(_))
        .withDefault(defaultDBLambdaName)

    val puaCallName: Opts[LambdaFunctionName] =
      Opts
        .option[String](
          "pua_call_lambda_name",
          "the name of the pua caller Lambda"
        )
        .map(LambdaFunctionName(_))
        .withDefault(defaultCallLambdaName)

    val paws: Opts[PuaAws] =
      (puaDBName, puaCallName).mapN(puaAws(_, _, aws, blocker))

    val poll: Opts[Option[FiniteDuration]] = {
      val block = (
        Opts.flag("block", "should we wait for synchonously for the result"),
        Opts
          .option[Int](
            "poll_sec",
            "how long to wait between polls, (10 sec default)"
          )
          .withDefault(10)
      ).mapN { (_, secs) =>
        Some(FiniteDuration(secs, "s"))
      }

      val noBlock = Opts(None)

      block.orElse(noBlock)
    }

    val invoke =
      Command("invoke", "invoke a pua") {
        (puaOpt, arg, paws, poll).mapN { (pi, ji, puaAws, pollDur) =>
          (pi, ji).mapN { (pua, arg) =>
            pollDur match {
              case None =>
                val fn = puaAws.toIOFn[Json](pua)
                for {
                  slot <- fn(arg)
                  _ <- IO(println(s"result in slot: $slot"))
                } yield ()
              case Some(dur) =>
                val fn = puaAws.toIOFnPoll[Json, Json](pua, dur)
                for {
                  jres <- fn(arg)
                  _ <- IO(println(jres.spaces4))
                } yield ()
            }
          }.flatten
        }
      }

    val read =
      Command("read", "read a slot if ready") {
        (Opts.option[Long]("slot", "slot id to read"), paws, poll).mapN {
          (slot, puaAws, pollDur) =>
            val value: IO[Option[Json]] = puaAws.pollSlot[Json](slot)

            pollDur match {
              case None =>
                value.flatMap {
                  case None =>
                    IO.raiseError(new Exception(s"slot $slot is not ready"))
                  case Some(j) =>
                    IO(println(j.spaces4))
                }
              case Some(dur) =>
                lazy val done: IO[Json] =
                  value.flatMap {
                    case None =>
                      logger.info("sleep for {} then retry {}", dur, slot)
                      IO.sleep(dur) *> done
                    case Some(j) => IO.pure(j)
                  }

                done.flatMap(j => IO(println(j.spaces4)))
            }
        }
      }

    val lambdaDeploy: IO[LambdaDeploy] =
      UniqueName.build[IO].map(new LambdaDeploy(aws, awsS3, _, blocker))

    val create =
      Command("create", "create the main pua AWS worker") {
        (
          LambdaDeploy.casRootOpts.withDefault(
            S3Addr("predictionmachine-data", "cas")
          ),
          puaDBName,
          puaCallName,
          LambdaDeploy.Coord.jarPath
        ).mapN { (s3root, dbname, callname, jarPath) =>
          val memSize: Int = 768 // we should tune this
          val env: Map[String, String] = makeArgs(
            DatabaseName("pmdb_prod"),
            region = "us-west-2",
            secret =
              "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres"
          )
          // this role/vpc needs to be able to see the DB
          val role =
            "arn:aws:iam::131579175100:role/hiona-FinnhubBars-functionRole-1DGE5DL8HGZRQ"
          val vpc = LambdaDeploy.Vpc(Set("subnet-108a2467"), Set("sg-2defd948"))

          val dbFn = LambdaDeploy.FunctionCreationArgs(
            LambdaDeploy.Coord(
              jarPath,
              LambdaDeploy.MethodName(classOf[LambdaPuaWorker].getName)
            ),
            Some(dbname),
            Some("lambda to store Pua state in DB"),
            Runtime.Java11,
            role,
            memSize,
            60, // DB operations that take 60 seconds are WAY too long
            Some(env),
            Some(vpc)
          )

          val callFn = LambdaDeploy.FunctionCreationArgs(
            LambdaDeploy.Coord(
              jarPath,
              LambdaDeploy.MethodName(classOf[LambdaPuaCaller].getName)
            ),
            Some(callname),
            Some("lambda to make calls to other lambdas to run Pua"),
            Runtime.Java11,
            role,
            memSize,
            15 * 60, // TODO: we are currently blocking, but could make this async... 15 min
            Some(makeCallerArgs(dbname)),
            None // no VPC, since VPC nodes can't call out
          )

          for {
            ld <- lambdaDeploy
            _ <- ld.createLambda(s3root, dbFn)
            _ <- ld.createLambda(s3root, callFn)
          } yield ()
        }
      }

    Command("pua_client", "an app to control pua") {
      Opts.subcommands(
        invoke,
        read,
        create
      )
    }
  }

}

object LambdaPuaClientApp extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val cmdRes = for {
      blocker <- Blocker[IO]
      aws <- LambdaDeploy.awsLambda
      awsS3 <- LambdaDeploy.awsS3
    } yield LambdaPuaWorkerState.command(aws, awsS3, blocker)

    cmdRes.use { cmd =>
      cmd.parse(args) match {
        case Right(io) => io.as(ExitCode.Success)
        case Left(err) =>
          IO {
            System.err.println(err.toString)
            ExitCode.Error
          }
      }
    }
  }
}

/**
  * This is a simple test lambda that invokes another lambda
  */
class ApplyLambda
    extends PureLambda[(AWSLambda, Blocker), (String, Json), Json] {

  implicit private val ctxIO: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  def setup =
    (LambdaDeploy.awsLambda, Blocker[IO]).mapN((_, _)).allocated.map(_._1)

  def run(
      in: (String, Json),
      state: (AWSLambda, Blocker),
      context: Context
  ): IO[Json] = {
    val (nm, arg) = in
    val fn = state._1.makeLambda(LambdaFunctionName(nm), state._2)
    fn(arg)
  }
}
