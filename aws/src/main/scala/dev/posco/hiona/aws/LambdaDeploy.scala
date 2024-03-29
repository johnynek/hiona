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

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Async, ExitCode, IO, IOApp, Resource, Temporal}
import com.amazonaws.PredefinedClientConfigurations
import com.amazonaws.services.lambda.model.{
  CreateFunctionRequest,
  DeleteFunctionRequest,
  Environment,
  FunctionCode,
  GetFunctionConfigurationRequest,
  InvokeRequest,
  Runtime,
  VpcConfig
}
import com.amazonaws.services.lambda.{AWSLambda, AWSLambdaClientBuilder}
import com.amazonaws.services.s3
import com.monovore.decline.{Argument, Command, Opts}
import dev.posco.hiona.{IOEnv, LazyToString}
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import io.circe.parser.decode
import java.nio.ByteBuffer
import java.nio.file.Path
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory
import scala.concurrent.duration.FiniteDuration

import cats.implicits._

class LambdaDeploy(
    awsLambda: AWSLambda,
    awsS3: s3.AmazonS3,
    unique: UniqueName[IO]
)(implicit ctx: Async[IO]) {

  import LambdaDeploy._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def createLambda(
      casRoot: S3Addr,
      funcArgs: FunctionCreationArgs
  ): IO[LambdaFunctionName] = {
    logger.info("creating ephemeral lambda for {}", funcArgs.coord)

    val s3AddrIO: IO[S3Addr] =
      ContentStore.put[IO](awsS3, casRoot, funcArgs.coord.jarPath)

    for {
      s3addr <- s3AddrIO
      (name, fa1) <- funcArgs.ensureNamed(unique)
      _ <- IO {
        logger.info(
          "creating ephemeral function {} with code at {}",
          name,
          s3addr
        )
      }
      _ <- IO.blocking(awsLambda.createFunction(fa1.toRequest(s3addr)))
    } yield name
  }

  def lambdaResource(
      casRoot: S3Addr,
      funcArgs: FunctionCreationArgs
  ): Resource[IO, LambdaFunctionName] = {

    val create =
      createLambda(casRoot, funcArgs)
        .flatMap { nm =>
          awsLambda
            .waitNonPending(nm)
            .as(nm)
        }

    Resource
      .make(create)(deleteLambda)
  }

  def deleteLambda(nm: LambdaFunctionName): IO[Unit] = {
    val name = nm.asString
    val dr = new DeleteFunctionRequest()
      .withFunctionName(name)

    IO(logger.info("deleting ephemeral function {}", name)) *>
      IO.blocking(awsLambda.deleteFunction(dr))
        .flatMap { resp =>
          val code = resp.getSdkHttpMetadata().getHttpStatusCode()
          checkCode(code) {
            logger.error("when deleting {} got code {}", name, code)
            s"error code: $code, trying to delete: $name"
          }
        }
  }

  def invokeRemoteAsync(
      casRoot: S3Addr,
      funcArgs: FunctionCreationArgs,
      in: Json
  ): IO[Unit] =
    for {
      name <- createLambda(casRoot, funcArgs)
      _ <- awsLambda.waitNonPending(name)
      _ <- awsLambda.makeLambdaAsync(name).apply(in)
    } yield ()

  def invokeRemoteSync(
      casRoot: S3Addr,
      funcArgs: FunctionCreationArgs,
      in: Json
  ): IO[Json] =
    lambdaResource(casRoot, funcArgs)
      .use(name => awsLambda.makeLambda(name).apply(in))

  def invokeRemoteKind(
      casRoot: S3Addr,
      funcArgs: FunctionCreationArgs,
      in: Json,
      kind: LambdaDeploy.InvocationKind
  ): IO[Unit] =
    kind match {
      case LambdaDeploy.InvocationKind.Sync =>
        invokeRemoteSync(casRoot, funcArgs, in)
          .flatMap { json =>
            IO(println(json.spaces2))
          }
      case LambdaDeploy.InvocationKind.Async =>
        invokeRemoteAsync(casRoot, funcArgs, in)
    }

  def invokeExisting(
      name: LambdaFunctionName,
      in: Json,
      kind: LambdaDeploy.InvocationKind
  ): IO[Unit] =
    kind match {
      case LambdaDeploy.InvocationKind.Sync =>
        awsLambda
          .makeLambda(name)
          .apply(in)
          .flatMap { json =>
            IO(println(json.spaces2))
          }
      case LambdaDeploy.InvocationKind.Async =>
        awsLambda
          .makeLambdaAsync(name)
          .apply(in)
    }

  val invokeRemoteCommand: Command[IO[Unit]] =
    Command("invoke_remote", "deploy, create, invoke, then delete")(
      (
        LambdaDeploy.optFunArgs,
        LambdaDeploy.casRootOpts,
        Payload.optPayload("payload"),
        LambdaDeploy.InvocationKind.invocationKindOpts
      ).mapN { (funcArgs, s3root, payload, ik) =>
        for {
          fa <- funcArgs
          j <- payload.toJson
          _ <- invokeRemoteKind(
            s3root,
            fa,
            j,
            ik
          )
        } yield ()
      }
    )

  val createCommand: Command[IO[Unit]] =
    Command("create", "create and deploy a lambda")(
      (
        LambdaDeploy.casRootOpts,
        LambdaDeploy.optFunArgs,
        Opts
          .flag("wait", "wait until the function is no longer pending")
          .orFalse
      ).mapN { (s3root, funcArgs, wait) =>
        for {
          fa <- funcArgs
          name <- createLambda(s3root, fa)
          _ <- if (wait) awsLambda.waitNonPending(name) else IO.unit
          _ <- IO(println(s"created: $name"))
        } yield ()
      }
    )

  val invokeExistingCommand: Command[IO[Unit]] =
    Command("invoke", "invoke an existing lambda")(
      (
        LambdaDeploy.optLambdaName,
        Payload.optPayload("payload"),
        LambdaDeploy.InvocationKind.invocationKindOpts
      ).mapN { (nm, in, ik) =>
        in.toJson.flatMap(invokeExisting(nm, _, ik))
      }
    )

  val deleteFunction: Command[IO[Unit]] =
    Command("delete", "delete a deployed lambda function")(
      Opts
        .option[String]("name", "the name of the lambda to delete")
        .map { nm =>
          deleteLambda(LambdaFunctionName(nm))
        }
    )

  val cmd: Command[IO[Unit]] =
    Command("lambdadeploy", "tool to deploy and invoke lambda functions")(
      Opts
        .subcommands(
          invokeRemoteCommand,
          deleteFunction,
          invokeExistingCommand,
          createCommand
        )
    )
}

object LambdaDeploy {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  case class MethodName(asString: String)
  object MethodName {
    implicit val methodNameArgument: Argument[MethodName] =
      new Argument[MethodName] {
        def defaultMetavar = "method_name"
        def read(s: String): ValidatedNel[String, MethodName] =
          // TODO: we could check that this parses correctly...
          Validated.valid(MethodName(s))
      }
  }

  case class Coord(jarPath: Path, methodName: MethodName)
  object Coord {
    val jarPath: Opts[Path] =
      Opts.option[Path]("jar", "the jar containing the code")

    val opts: Opts[Coord] =
      (jarPath, Opts.option[MethodName]("method", "the lambda function method"))
        .mapN(Coord(_, _))
  }

  case class Vpc(subnets: Set[String], securityGroups: Set[String]) {
    def toVpcConfig: VpcConfig =
      new VpcConfig()
        .withSubnetIds(subnets.toList.sorted: _*)
        .withSecurityGroupIds(securityGroups.toList.sorted: _*)
  }
  object Vpc {
    val vpcOpts: Opts[Option[Vpc]] =
      (
        Opts.options[String]("subnet", "vpc subnet").orEmpty,
        Opts.options[String]("sec_group", "security groups").orEmpty
      ).mapN {
        case (Nil, Nil) => None
        case (sn, sg)   => Some(Vpc(sn.toSet, sg.toSet))
      }
  }

  case class FunctionCreationArgs(
      coord: Coord,
      name: Option[LambdaFunctionName],
      description: Option[String],
      runtime: Runtime,
      role: String,
      memSize: Int,
      timeout: Int,
      env: Option[Map[String, String]],
      vpc: Option[LambdaDeploy.Vpc]
  ) {

    def ensureNamed(
        unique: UniqueName[IO]
    ): IO[(LambdaFunctionName, FunctionCreationArgs)] =
      name match {
        case Some(n) => IO.pure((n, this))
        case None =>
          unique.next.map { nm =>
            val lfn = LambdaFunctionName(nm)
            (lfn, copy(name = Some(lfn)))
          }
      }

    def toRequest(s3addr: S3Addr): CreateFunctionRequest = {
      def withOpt[A](opt: Option[A])(
          fn: (CreateFunctionRequest, A) => CreateFunctionRequest
      ): CreateFunctionRequest => CreateFunctionRequest = { cfr =>
        opt.fold(cfr)(fn(cfr, _))
      }

      val wenv = withOpt(env) { (req, env) =>
        val e = env.toList.sortBy(_._1).foldLeft(new Environment()) {
          case (e, (k, v)) =>
            e.addVariablesEntry(k, v)
        }
        req.withEnvironment(e)
      }

      val wvpc = withOpt(vpc) { (req, vpc) =>
        req.withVpcConfig(vpc.toVpcConfig)
      }

      val desc1 =
        if (description.isEmpty)
          Some(s"function for ${coord.methodName} at $s3addr")
        else description

      val fns = List(
        withOpt(name.map(_.asString))(_.withFunctionName(_)),
        withOpt(desc1)(_.withDescription(_)),
        wenv,
        wvpc
      )

      val req0 = new CreateFunctionRequest()
        .withHandler(coord.methodName.asString)
        .withCode(
          new FunctionCode()
            .withS3Bucket(s3addr.bucket)
            .withS3Key(s3addr.key)
        )
        .withRole(role)
        .withMemorySize(memSize)
        .withTimeout(timeout)
        .withRuntime(runtime)

      fns.foldLeft(req0)((r, fn) => fn(r))
    }
  }

  val optLambdaName: Opts[LambdaFunctionName] =
    Opts
      .option[String]("name", "the name for the lambda")
      .map(LambdaFunctionName(_))

  val roleOpt: Opts[String] =
    Opts.option[String]("role", "the arn of the role to use to invoke")

  val runtimeOpts: Opts[Runtime] =
    Opts
      .option[String]("runtime", "the lambda runtime to use (default: Java11)")
      .withDefault("Java11")
      .mapValidated { str =>
        try Validated.valid(Runtime.valueOf(str))
        catch {
          case _: IllegalArgumentException =>
            val valid = Runtime.values.toList.map(_.toString).sorted
            Validated.invalidNel(
              s"invalid runtime: $str, expected: ${valid.mkString(", ")}"
            )
        }
      }

  val optFunArgs: Opts[IO[FunctionCreationArgs]] =
    (
      LambdaDeploy.Coord.opts,
      optLambdaName.orNone,
      Opts.option[String]("desc", "description of the lambda").orNone,
      runtimeOpts,
      roleOpt,
      Opts
        .option[Int](
          "memsize",
          "maximum MB of the lambda can use (3008 MB default)"
        )
        .withDefault(3008),
      Opts
        .option[Int](
          "timeout",
          "maximum number of seconds of the lambda can use (900s = 15m default)"
        )
        .withDefault(15 * 60),
      Payload.optPayload("env").orNone,
      LambdaDeploy.Vpc.vpcOpts
    ).mapN {
      (coord, lambdaNameOpt, desc, runtime, role, mem, timeout, env, optVpc) =>
        for {
          ejson <- env.traverse(_.toJson)
          envMap <- ejson.traverse { j =>
            IO.fromEither(j.as[Map[String, String]])
          }
        } yield FunctionCreationArgs(
          coord,
          lambdaNameOpt,
          desc,
          runtime,
          role,
          mem,
          timeout,
          envMap,
          optVpc
        )
    }

  val casRootOpts: Opts[S3Addr] =
    Opts.option[S3Addr](
      "cas_root",
      "s3 uri to root of the content-addressed store"
    )

  sealed abstract class FnState
  object FnState {
    case object Active extends FnState
    case object Inactive extends FnState
    case object Failed extends FnState
    case object Pending extends FnState

    def fromString(str: String): Option[FnState] =
      str match {
        case "Active"   => Some(Active)
        case "Inactive" => Some(Inactive)
        case "Failed"   => Some(Failed)
        case "Pending"  => Some(Pending)
        case _          => None
      }
  }

  sealed abstract class InvocationKind(val asString: String)
  object InvocationKind {
    case object Sync extends InvocationKind("RequestResponse")
    case object Async extends InvocationKind("Event")

    val invocationKindOpts: Opts[InvocationKind] =
      Opts
        .flag(
          "async",
          "invoke asynchronously and do not delete the function after the call"
        )
        .orTrue
        .map {
          case false => InvocationKind.Async
          case true  => InvocationKind.Sync
        }
  }

  def checkCode(code: Int)(msg: => String): IO[Unit] =
    if ((200 <= code) && (code <= 299)) IO.unit
    else IO.raiseError(new Exception(msg))

  implicit class LambdaMethods(private val awsLambda: AWSLambda)
      extends AnyVal {

    def getState(name: LambdaFunctionName): IO[FnState] =
      IO.blocking {
        val arg = new GetFunctionConfigurationRequest()
          .withFunctionName(name.asString)
        awsLambda.getFunctionConfiguration(arg)
      }.flatMap { conf =>
        FnState.fromString(conf.getState) match {
          case Some(fn) => IO.pure(fn)
          case None =>
            IO.raiseError(new Exception(s"unknown function state in: $conf"))
        }
      }

    def waitNonPending(
        name: LambdaFunctionName,
        retries: Int = 10,
        nextSleep: FiniteDuration = FiniteDuration(10, "s")
    )(implicit timer: Temporal[IO]): IO[FnState] =
      getState(name)
        .flatMap {
          case FnState.Pending =>
            if (retries <= 0) {
              logger.error(
                "function name {} exhausted retries in Pending state",
                name.asString
              )
              IO.raiseError(
                new Exception(s"$name exhausted retries in Pending state")
              )
            } else {
              logger.info(
                "function name {} still Pending with {} remaining retries",
                name.asString,
                retries
              )
              val nextNext = (nextSleep * 15L) / 10L
              IO.sleep(nextSleep) *> waitNonPending(
                name,
                retries - 1,
                nextNext
              )
            }

          case nonPending => IO.pure(nonPending)
        }

    def makeLambda(name: LambdaFunctionName): Json => IO[Json] =
      makeLambdaInternal(
        name,
        InvocationKind.Sync,
        bb => IO.fromTry(CirceSupportParser.parseFromByteBuffer(bb))
      )

    def makeLambdaAsync(name: LambdaFunctionName): Json => IO[Unit] =
      makeLambdaInternal(
        name,
        InvocationKind.Async,
        _ =>
          IO(
            logger.info(
              "invoking: {}, as an event. Function is not deleted after the call.",
              name
            )
          )
      )

    private def makeLambdaInternal[A](
        name: LambdaFunctionName,
        kind: InvocationKind,
        onPayload: ByteBuffer => IO[A]
    ): Json => IO[A] = { payload: Json =>
      val ir = new InvokeRequest()
        .withFunctionName(name.asString)
        .withPayload(payload.noSpaces)
        .withInvocationType(kind.asString)

      logger.info(
        "invoking function {} {} with argument {}\nrequest: {}",
        name,
        kind,
        payload,
        ir
      )

      val invoke = IO.blocking(awsLambda.invoke(ir))

      invoke
        .flatMap { resp =>
          val executed = resp.getExecutedVersion()
          val code = resp.getStatusCode()

          val decodeLog = LazyToString {
            val b64 = new Base64
            resp.getLogResult match {
              case null => "<null>"
              case notNull =>
                new String(b64.decode(notNull), "UTF-8")
            }
          }

          val fnError = resp.getFunctionError

          logger.info(
            "executed function name: {} version: {} status: {} function_error: {}\nlog:\n{}",
            name,
            executed,
            code,
            fnError,
            decodeLog
          )

          val payload = resp.getPayload

          val err =
            fnError match {
              case null => IO.unit
              case someError =>
                val str = new String(payload.duplicate.array(), "UTF-8")
                logger.error(
                  "function error: {} with payload: {}",
                  someError,
                  str
                )
                IO.raiseError(
                  new Exception(s"function error: $someError, payload = $str")
                )
            }

          val check = checkCode(code) {
            s"error code: $code, on: $executed\n\n${fnError}"
          }

          val result =
            onPayload(payload.duplicate)
              .onError {
                case t =>
                  logger.error(
                    "function name {} could not decode response {}.",
                    name,
                    new String(payload.array(), "UTF-8"),
                    t
                  )
                  IO.raiseError(t)
              }

          err *> check *> result
        }
    }
  }

  sealed trait Payload {
    def toJson: IO[Json]
  }
  object Payload {
    case class Literal(str: String) extends Payload {
      def toJson = IO.fromEither(decode[Json](str))
    }
    case class FromPath(path: Path) extends Payload {
      def toJson =
        IO.defer {
          IO.fromTry(CirceSupportParser.parseFromFile(path.toFile))
        }
    }

    def optPayload(nm: String): Opts[Payload] =
      Opts
        .option[String](nm, s"a literal json value to use as the $nm")
        .map(Payload.Literal(_))
        .orElse(
          Opts
            .option[Path](
              s"${nm}_path",
              s"the path containing json to use as the $nm"
            )
            .map(Payload.FromPath(_))
        )
  }

  val awsLambda: Resource[IO, AWSLambda] =
    Resource.make(IO {
      val bldr = AWSLambdaClientBuilder.standard()
      // allow us to wait for 15 minutes to allow synchronous
      // calls to any function
      val timeout = 15 * 60 * 1000
      val cconf =
        Option(bldr.getClientConfiguration)
          .getOrElse(PredefinedClientConfigurations.defaultConfig)
          .withConnectionMaxIdleMillis(timeout.toLong)
          .withConnectionTimeout(timeout)
          .withSocketTimeout(timeout)
          .withRequestTimeout(timeout)

      bldr.withClientConfiguration(cconf).build
    })(awsl => IO(awsl.shutdown()))

}

object LambdaDeployApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    (
      LambdaDeploy.awsLambda,
      AWSIO.awsS3,
      Resource.eval(UniqueName.build[IO])
    ).mapN(new LambdaDeploy(_, _, _))
      .use { ld =>
        IOEnv.read.flatMap { env =>
          ld.cmd.parse(args, env) match {
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
