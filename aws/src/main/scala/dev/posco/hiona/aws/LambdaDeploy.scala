package dev.posco.hiona.aws

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import dev.posco.hiona.LazyToString
import java.nio.file.Path
import com.monovore.decline.{Argument, Command, Opts}
import com.amazonaws.PredefinedClientConfigurations
import com.amazonaws.services.lambda.{AWSLambda, AWSLambdaClientBuilder}
import com.amazonaws.services.lambda.model.{
  CreateFunctionRequest,
  DeleteFunctionRequest,
  FunctionCode,
  GetFunctionConfigurationRequest,
  InvokeRequest,
  VpcConfig
}
import com.amazonaws.services.lambda.model
import com.amazonaws.services.s3
import io.circe.Json
import io.circe.parser.decode
import io.circe.jawn.CirceSupportParser
import java.nio.ByteBuffer
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory
import scala.concurrent.duration.FiniteDuration

import cats.implicits._

class LambdaDeploy(
    awsLambda: AWSLambda,
    awsS3: s3.AmazonS3,
    unique: UniqueName[IO],
    blocker: Blocker
)(implicit ctx: ContextShift[IO], timer: Timer[IO]) {

  import LambdaDeploy._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private def block[A](a: => A): IO[A] =
    blocker.blockOn(IO(a))

  def lambdaResource(
      coord: Coord,
      casRoot: S3Addr,
      role: String,
      optVpc: Option[LambdaDeploy.Vpc]
  ): Resource[IO, LambdaFunctionName] = {
    logger.info("creating ephemeral lambda for {}", coord)

    val s3AddrIO =
      ContentStore.put[IO](awsS3, casRoot, coord.jarPath, blocker)

    val ioRes = (unique.next, s3AddrIO).mapN { (name, s3addr) =>
      val req0 = new CreateFunctionRequest()
        .withHandler(coord.methodName.asString)
        .withFunctionName(name)
        .withCode(
          new FunctionCode()
            .withS3Bucket(s3addr.bucket)
            .withS3Key(s3addr.key)
        )
        .withDescription(s"ad-hoc, emphemeral hiona function: $name")
        .withMemorySize(3008)
        .withTimeout(900)
        .withRuntime(model.Runtime.Java11)
        .withRole(role)

      val req = optVpc match {
        case None => req0
        case Some(vpc) =>
          val vpcConfig = new VpcConfig()
            .withSubnetIds(vpc.subnets.toList.sorted: _*)
            .withSecurityGroupIds(vpc.securityGroups.toList.sorted: _*)

          req0.withVpcConfig(vpcConfig)
      }

      val create =
        IO(
          logger.info(
            "creating ephemeral function {} with code at {}",
            name,
            s3addr
          )
        ) *>
          block(awsLambda.createFunction(req))

      val nm = LambdaFunctionName(name)
      val nonPending = awsLambda.waitNonPending(nm, blocker)

      Resource
        .make(create *> nonPending)(_ => deleteLambda(nm))
        .as(nm)
    }

    Resource.liftF(ioRes).flatten
  }

  def deleteLambda(nm: LambdaFunctionName): IO[Unit] = {
    val name = nm.asString
    val dr = new DeleteFunctionRequest()
      .withFunctionName(name)

    IO(logger.info("deleting ephemeral function {}", name)) *>
      block(awsLambda.deleteFunction(dr))
        .flatMap { resp =>
          val code = resp.getSdkHttpMetadata().getHttpStatusCode()
          checkCode(code) {
            logger.error("when deleting {} got code {}", name, code)
            s"error code: $code, trying to delete: $name"
          }
        }
  }

  def invokeRemoteAsync(
      coord: Coord,
      casRoot: S3Addr,
      role: String,
      in: Json,
      optVpc: Option[LambdaDeploy.Vpc]
  ): IO[Unit] =
    lambdaResource(coord, casRoot, role, optVpc).allocated
      .flatMap {
        case (name, _) =>
          awsLambda.makeLambdaAsync(name, blocker).apply(in)
      }

  def invokeRemoteSync(
      coord: Coord,
      casRoot: S3Addr,
      role: String,
      in: Json,
      optVpc: Option[LambdaDeploy.Vpc]
  ): IO[Json] =
    lambdaResource(coord, casRoot, role, optVpc)
      .use(name => awsLambda.makeLambda(name, blocker).apply(in))

  def invokeRemoteKind(
      coord: Coord,
      casRoot: S3Addr,
      role: String,
      in: Json,
      kind: LambdaDeploy.InvocationKind,
      optVpc: Option[LambdaDeploy.Vpc]
  ): IO[Unit] =
    kind match {
      case LambdaDeploy.InvocationKind.Sync =>
        invokeRemoteSync(coord, casRoot, role, in, optVpc)
          .flatMap { json =>
            IO(println(json.spaces2))
          }
      case LambdaDeploy.InvocationKind.Async =>
        invokeRemoteAsync(coord, casRoot, role, in, optVpc)
    }
  val invokeRemoteCommand: Command[IO[Unit]] =
    Command("invoke_remote", "deploy, create, invoke, then delete")(
      (
        Opts.option[Path]("jar", "the jar containing the code"),
        Opts.option[MethodName]("method", "the lambda function method"),
        Opts.option[S3Addr](
          "cas_root",
          "s3 uri to root of the content-addressed store"
        ),
        Opts.option[String]("role", "the arn of the role to use to invoke"),
        Payload.optPayload,
        LambdaDeploy.InvocationKind.invocationKindOpts,
        LambdaDeploy.Vpc.vpcOpts
      ).mapN { (p, mn, s3root, role, in, ik, optVpc) =>
        for {
          j <- in.toJson
          _ <- invokeRemoteKind(Coord(p, mn), s3root, role, j, ik, optVpc)
        } yield ()
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
      Opts.subcommands(invokeRemoteCommand, deleteFunction)
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

  case class Vpc(subnets: Set[String], securityGroups: Set[String])
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

    def getState(name: LambdaFunctionName, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
    ): IO[FnState] =
      blocker
        .blockOn(IO {
          val arg = new GetFunctionConfigurationRequest()
            .withFunctionName(name.asString)
          awsLambda.getFunctionConfiguration(arg)
        })
        .flatMap { conf =>
          FnState.fromString(conf.getState) match {
            case Some(fn) => IO.pure(fn)
            case None =>
              IO.raiseError(new Exception(s"unknown function state in: $conf"))
          }
        }

    def waitNonPending(
        name: LambdaFunctionName,
        blocker: Blocker,
        retries: Int = 10,
        nextSleep: FiniteDuration = FiniteDuration(10, "s")
    )(implicit ctx: ContextShift[IO], timer: Timer[IO]): IO[FnState] =
      getState(name, blocker)
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
                blocker,
                retries - 1,
                nextNext
              )
            }

          case nonPending => IO.pure(nonPending)
        }

    def makeLambda(name: LambdaFunctionName, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
    ): Json => IO[Json] =
      makeLambdaInternal(
        name,
        blocker,
        InvocationKind.Sync,
        bb => IO.fromTry(CirceSupportParser.parseFromByteBuffer(bb))
      )

    def makeLambdaAsync(name: LambdaFunctionName, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
    ): Json => IO[Unit] =
      makeLambdaInternal(
        name,
        blocker,
        InvocationKind.Async,
        _ =>
          IO(
            println(
              s"invoking: $name, as an event. Function is not deleted after the call."
            )
          )
      )

    private def makeLambdaInternal[A](
        name: LambdaFunctionName,
        blocker: Blocker,
        kind: InvocationKind,
        onPayload: ByteBuffer => IO[A]
    )(implicit
        ctx: ContextShift[IO]
    ): Json => IO[A] = { payload: Json =>
      val ir = new InvokeRequest()
        .withFunctionName(name.asString)
        .withPayload(payload.noSpaces)
        .withInvocationType(kind.asString)

      logger.info("invoking function {} with argument {}", name, payload)

      val invoke = blocker.blockOn(IO(awsLambda.invoke(ir)))

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
        IO.suspend {
          IO.fromTry(CirceSupportParser.parseFromFile(path.toFile))
        }
    }

    val optPayload =
      Opts
        .option[String]("payload", "a literal json value to use as the arg")
        .map(Payload.Literal(_))
        .orElse(
          Opts
            .option[Path]("payload_path", "the path containing Json")
            .map(Payload.FromPath(_))
        )

  }
}

object LambdaDeployApp extends IOApp {

  val awsLambda: Resource[IO, AWSLambda] =
    Resource.make(IO {
      val bldr = AWSLambdaClientBuilder.standard()
      // allow us to wait for 15 minutes to allow synchronous
      // calls to any function
      val timeout = 15 * 60 * 1000
      val cconf =
        Option(bldr.getClientConfiguration)
          .getOrElse(PredefinedClientConfigurations.defaultConfig)
          .withConnectionMaxIdleMillis(timeout)
          .withConnectionTTL(timeout)
          .withRequestTimeout(timeout)

      bldr.withClientConfiguration(cconf).build
    })(awsl => IO(awsl.shutdown()))

  val awsS3: Resource[IO, s3.AmazonS3] =
    Resource.make(IO {
      s3.AmazonS3ClientBuilder.defaultClient()
    })(awsS3 => IO(awsS3.shutdown()))

  def run(args: List[String]): IO[ExitCode] =
    (awsLambda, awsS3, Resource.liftF(UniqueName.build[IO]), Blocker[IO])
      .mapN(new LambdaDeploy(_, _, _, _))
      .use { ld =>
        ld.cmd.parse(args) match {
          case Right(io) => io.as(ExitCode.Success)
          case Left(err) =>
            IO {
              System.err.println(err.toString)
              ExitCode.Error
            }
        }
      }
}
