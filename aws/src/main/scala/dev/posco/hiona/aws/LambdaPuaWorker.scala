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

import cats.effect.Temporal
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.amazonaws.services.lambda.AWSLambda
import com.amazonaws.services.lambda.model.Runtime
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.s3
import com.monovore.decline.{Command, Opts}
import dev.posco.hiona.IOEnv
import io.circe.Json
import scala.concurrent.duration.FiniteDuration

import cats.implicits._

import LambdaDeploy.LambdaMethods
import RDSTransactor.DatabaseName

final class LambdaPuaWorker extends PuaWorker {
  def setup =
    for {
      (db, region, sec) <- IOEnv.readArgs(LambdaPuaWorkerState.dbEnvCmd)
      // we are ignoring the shutdown code, since the lambda gives no hooks for this
      (state, _) <- LambdaPuaWorkerState.state(db, region, sec).allocated
    } yield state
}

final class LambdaPuaCaller extends PuaCaller {
  def setup =
    for {
      dbFnName <- IOEnv.readArgs(LambdaPuaWorkerState.callerEnvCmd)
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

  def makeFunctions(awsLambda: AWSLambda): (
      LambdaFunctionName => Json => IO[Json],
      LambdaFunctionName => Json => IO[Unit]
  ) = {
    val syncFn = { nm: LambdaFunctionName => awsLambda.makeLambda(nm) }
    val asyncFn = { nm: LambdaFunctionName =>
      awsLambda.makeLambdaAsync(nm)
    }

    (syncFn, asyncFn)
  }

  def state(
      puaDB: DatabaseName,
      region: String,
      dbSecret: String
  ): Resource[IO, PuaWorker.State] =
    for {
      trans <- Resource.eval(
        RDSTransactor
          .build[IO](region, dbSecret, puaDB)
      )
      pg = new PostgresDBControl(trans)
    } yield PuaWorker.State(pg)

  def callerState(dbFn: LambdaFunctionName): Resource[IO, PuaCaller.State] =
    for {
      awsLambda <- LambdaDeploy.awsLambda
      (sync, async) = makeFunctions(awsLambda)
    } yield PuaCaller.State(dbFn, sync, async)

  def puaAws(
      puaDB: LambdaFunctionName,
      puaCaller: LambdaFunctionName,
      aws: AWSLambda
  ): PuaAws = {
    val (sync, async) = makeFunctions(aws)
    new PuaAws(PuaAws.Invoke.fromSyncAsyncNames(sync, async, puaDB, puaCaller))
  }

  def command(aws: AWSLambda, awsS3: s3.AmazonS3)(implicit
      timer: Temporal[IO]
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
      (puaDBName, puaCallName).mapN(puaAws(_, _, aws))

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
      UniqueName.build[IO].map(new LambdaDeploy(aws, awsS3, _))

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
      aws <- LambdaDeploy.awsLambda
      awsS3 <- AWSIO.awsS3
    } yield LambdaPuaWorkerState.command(aws, awsS3)

    cmdRes.use { cmd =>
      IOEnv.read.flatMap { env =>
        cmd.parse(args, env) match {
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
}

/** This is a simple test lambda that invokes another lambda */
class ApplyLambda extends PureLambda[AWSLambda, (String, Json), Json] {

  def setup =
    LambdaDeploy.awsLambda.allocated.map(_._1)

  def run(
      in: (String, Json),
      state: AWSLambda,
      context: Context
  ): IO[Json] = {
    val (nm, arg) = in
    val fn = state.makeLambda(LambdaFunctionName(nm))
    fn(arg)
  }
}
