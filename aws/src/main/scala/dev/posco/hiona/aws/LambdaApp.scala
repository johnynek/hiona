package dev.posco.hiona.aws

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ContextShift, IO, Resource}
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.s3
import com.monovore.decline.{Argument, Opts}
import dev.posco.hiona.IOEnv
import java.util.concurrent.Executors
import io.circe.{Decoder, HCursor, Json}
import scala.concurrent.ExecutionContext
import doobie.Transactor

import dev.posco.hiona._
import cats.effect.ExitCode

import cats.implicits._

object LambdaApp {
  case class BodyArgs(args: List[String])

  object BodyArgs {
    implicit val bodyArgsDecoder: Decoder[BodyArgs] =
      new Decoder[BodyArgs] {
        def apply(c: HCursor): Decoder.Result[BodyArgs] =
          c.downField("body")
            .downField("args")
            .as[List[String]]
            .map(BodyArgs(_))
      }
  }
}

class LambdaApp[A](opts: Opts[A], appOutput: A => Output)
    extends PureLambda[S3App, Json, Json] {

  def setup = IO(new S3App)

  def parseOutput(input: Json): Either[String, List[String]] =
    input.as[LambdaApp.BodyArgs] match {
      case Right(LambdaApp.BodyArgs(items)) => Right(items)
      case Left(err) =>
        Left(s"body.args to be JArray in top-level $input, got: $err")
    }

  final def run(
      json: Json,
      s3App: S3App,
      context: Context
  ): IO[Json] =
    IO.suspend {
      val logger = context.getLogger()

      implicit val ctx = s3App.contextShift

      for {
        stringOutput <-
          IO.fromEither(parseOutput(json).leftMap(new Exception(_)))
        _ = logger.log(s"INFO: parsed input: $stringOutput")
        cmd = s3App.commandWith(s3App.command, opts)
        env <- IOEnv.read
        eitherCmd = cmd.parse(stringOutput, env)
        (cmd, arg) <- IO.fromEither(eitherCmd.leftMap { msg =>
          new Exception(s"couldn't parse args\n$msg")
        })
        _ = logger.log(s"INFO: about to run")
        _ <- cmd.run(appOutput(arg), s3App.blocker)
        _ = logger.log(s"INFO: finished run")
      } yield Json.fromString("done")
    }
}

class LambdaApp0(output: Output) extends LambdaApp[Unit](Opts.unit, _ => output)

class S3App extends GenApp {
  type Ref = S3Addr

  val s3Client = s3.AmazonS3ClientBuilder.defaultClient()
  val awsIO = new AWSIO(s3Client)

  /**
    *  we only want one of these for the whole life of the lambda
    *
    *  These are used to handle blocking IO, so we don't want to
    *  use the CPU-scaled execution context for those
    */
  val blocker: Blocker =
    Blocker.liftExecutionContext(
      ExecutionContext.fromExecutor(
        Executors.newCachedThreadPool { (r: Runnable) =>
          val t = new Thread(r)
          // we always wait for all these
          // threads, so we can consider them daemon threads
          t.setDaemon(true)
          t
        }
      )
    )
  val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  implicit def argumentForRef: Argument[S3Addr] =
    new Argument[S3Addr] {
      val defaultMetavar = "s3uri"
      def read(s: String): ValidatedNel[String, S3Addr] =
        if (!s.startsWith("s3://"))
          Validated.invalidNel(
            s"string $s expected to start with s3:// character, not found"
          )
        else {
          val tail = s.substring(5)
          val slash = tail.indexOf('/')
          if (slash < 0)
            Validated.invalidNel(
              s"string $s expected to have / to separate bucket/key. not found"
            )
          else {
            val bucket = tail.substring(0, slash)
            val key = tail.substring(slash + 1)
            Validated.valid(S3Addr(bucket, key))
          }
        }
    }

  def read[A](input: S3Addr, row: Row[A], blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): fs2.Stream[IO, A] =
    awsIO
      .readStream[IO](input, 1 << 16, blocker)
      .through(fs2.text.utf8Decode)
      .through(Row.decodeFromCSV[IO, A](row, skipHeader = true))

  def inputFactory[E[_]: Emittable, A](
      inputs: Iterable[(String, S3Addr)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, InputFactory[IO]] =
    Resource.pure[IO, InputFactory[IO]](InputFactory.fromMany(inputs, e) {
      (src, s3path) =>
        def go[T](src: Event.Source[T]) =
          InputFactory.fromStream[IO, T](src, read(s3path, src.row, blocker))

        go(src)
    })

  def writer[A](
      output: S3Addr,
      row: Row[A]
  ): Resource[IO, Iterator[A] => IO[Unit]] =
    //awsIO.tempWriter(output, row)
    awsIO.multiPartOutput(output, row)
}

abstract class DBS3App extends S3App {

  def transactor: Resource[IO, doobie.Transactor[IO]]

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def dbSupportFactory: IO[db.DBSupport.Factory]

  final lazy val dbInputFactory: Resource[IO, InputFactory[IO]] =
    for {
      t <- transactor
      dbsf <- Resource.liftF(dbSupportFactory)
    } yield dbsf.build(t)

  override def inputFactory[E[_]: Emittable, A](
      inputs: Iterable[(String, S3Addr)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, InputFactory[IO]] =
    (dbInputFactory, super.inputFactory(inputs, e, blocker)).mapN(_.combine(_))
}

abstract class DBS3CliApp extends DBS3App {
  def eventOutput: Output

  def runIO(args: List[String]): IO[ExitCode] =
    IO.suspend {
      IOEnv.read.flatMap { env =>
        command.parse(args, env) match {
          case Right(cmd) =>
            implicit val ictx = contextShift
            cmd.run(eventOutput, blocker)
          case Left(err) =>
            IO {
              System.err.println(err)
              ExitCode.Error
            }
        }
      }
    }

  final def main(args: Array[String]): Unit =
    System.exit(runIO(args.toList).unsafeRunSync().code)
}

abstract class DBLambdaApp0(
    appOutput: Output,
    dbsf: IO[db.DBSupport.Factory],
    trans: (Blocker, ContextShift[IO]) => IO[Transactor[IO]]
) extends LambdaApp0(appOutput) {
  override def setup =
    IO {
      new aws.DBS3App {
        def dbSupportFactory = dbsf
        val transactor = Resource.liftF(trans(blocker, contextShift))
      }
    }
}
