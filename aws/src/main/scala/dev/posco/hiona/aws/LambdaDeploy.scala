package dev.posco.hiona.aws

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource}
import java.nio.file.Path
import com.monovore.decline.{Argument, Command, Opts}
import com.amazonaws.services.lambda.{AWSLambda, AWSLambdaClientBuilder}
import com.amazonaws.services.lambda.model.{
  CreateFunctionRequest,
  DeleteFunctionRequest,
  FunctionCode,
  InvokeRequest
}
import com.amazonaws.services.lambda.model
import com.amazonaws.services.s3
import io.circe.Json
import io.circe.parser.decode
import io.circe.jawn.CirceSupportParser

import cats.implicits._

class LambdaDeploy(
    awsLambda: AWSLambda,
    awsS3: s3.AmazonS3,
    unique: UniqueName[IO],
    blocker: Blocker
)(implicit ctx: ContextShift[IO]) {

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

  case class FunctionName(asString: String)

  private def checkCode(code: Int)(msg: => String): IO[Unit] =
    if ((200 <= code) && (code <= 299)) IO.unit
    else IO.raiseError(new Exception(msg))

  def makeLambda(name: FunctionName): Json => IO[Json] = { payload: Json =>
    val ir = new InvokeRequest()
      .withFunctionName(name.asString)
      .withPayload(payload.noSpaces)

    block(awsLambda.invoke(ir))
      .flatMap { resp =>
        val executed = resp.getExecutedVersion()
        val code = resp.getStatusCode()
        checkCode(code) {
          s"error code: $code, on: $executed\n\n${resp.getFunctionError()}"
        } >>
          IO.fromTry(
            CirceSupportParser.parseFromByteBuffer(resp.getPayload)
          )
      }
  }

  private def block[A](a: => A): IO[A] =
    blocker.blockOn(IO(a))

  def lambdaResource(
      coord: Coord,
      casRoot: S3Addr,
      role: String
  ): Resource[IO, FunctionName] = {
    val s3AddrIO =
      ContentStore.put[IO](awsS3, casRoot, coord.jarPath, blocker)

    val ioRes = (unique.next, s3AddrIO).mapN { (name, s3addr) =>
      val req = new CreateFunctionRequest()
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

      val create = block(awsLambda.createFunction(req))

      val dr = new DeleteFunctionRequest()
        .withFunctionName(name)

      val delete: IO[Unit] =
        block(awsLambda.deleteFunction(dr))
          .flatMap { resp =>
            val code = resp.getSdkHttpMetadata().getHttpStatusCode()
            checkCode(code) {
              s"error code: $code, trying to delete: $name"
            }
          }

      Resource
        .make(create)(_ => delete)
        .as(FunctionName(name))
    }

    Resource.liftF(ioRes).flatten
  }

  def invokeRemote(
      coord: Coord,
      casRoot: S3Addr,
      role: String,
      in: Json
  ): IO[Json] =
    lambdaResource(coord, casRoot, role)
      .use(name => makeLambda(name)(in))

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
        Payload.optPayload
      ).mapN { (p, mn, s3root, role, in) =>
        for {
          j <- in.toJson
          jres <- invokeRemote(Coord(p, mn), s3root, role, j)
          _ <- IO(println(jres.spaces2))
        } yield ()
      }
    )

  val cmd: Command[IO[Unit]] =
    Command("lambdadeploy", "tool to deploy and invoke lambda functions")(
      Opts.subcommands(invokeRemoteCommand)
    )
}

object LambdaDeployApp extends IOApp {

  val awsLambda: Resource[IO, AWSLambda] =
    Resource.make(IO {
      AWSLambdaClientBuilder.defaultClient()
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
