package dev.posco.hiona.aws

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ContextShift, IO, Resource}
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.amazonaws.services.s3
import com.monovore.decline.Argument
import java.io.{InputStream, OutputStream}
import java.nio.channels.Channels
import java.util.concurrent.Executors
import io.circe.{Decoder, HCursor, Json}
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import dev.posco.hiona._
import cats.effect.ExitCode

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

abstract class LambdaApp(appArgs: Args) extends RequestStreamHandler {

  protected def buildS3App(): S3App = new S3App

  // allocating this initializes s3clients
  private[this] lazy val s3App = buildS3App()

  def parseArgs(input: Json): Either[String, List[String]] =
    input.as[LambdaApp.BodyArgs] match {
      case Right(LambdaApp.BodyArgs(items)) => Right(items)
      case Left(err) =>
        Left(s"body.args to be JArray in top-level $input, got: $err")
    }

  def handleRequest(
      inputStream: InputStream,
      outputStream: OutputStream,
      context: Context
  ): Unit = {
    val logger = context.getLogger()

    try {
      implicit val ctx = s3App.contextShift

      val inChannel = Channels.newChannel(inputStream)
      val jinput: Json =
        io.circe.jawn.CirceSupportParser.parseFromChannel(inChannel).get

      parseArgs(jinput) match {
        case Right(stringArgs) =>
          logger.log(s"INFO: parsed input: $stringArgs")

          s3App.command.parse(stringArgs) match {
            case Right(cmd) =>
              logger.log(s"INFO: about to run")
              cmd.run(appArgs, s3App.blocker).unsafeRunSync()
              logger.log(s"INFO: finished run")
              ()
              val result = Json.fromString("done")
              outputStream.write(result.spaces2.getBytes("UTF-8"))
              ()
            case Left(err) =>
              logger.log(s"ERROR: failed to parse command: $err")
          }
        case Left(msg) =>
          logger.log(
            s"ERROR: failed to parse input. $msg from: ${jinput.spaces2}"
          )
      }
    } catch {
      case NonFatal(e) =>
        logger.log(s"ERROR: $e")
    } finally {
      inputStream.close()
      outputStream.close()
    }
  }
}

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
  )(implicit ctx: ContextShift[IO]): InputFactory[IO] =
    InputFactory.fromMany(inputs, e) { (src, s3path) =>
      def go[T](src: Event.Source[T]) =
        InputFactory.fromStream[IO, T](src, read(s3path, src.row, blocker))

      go(src)
    }

  def writer[A](
      output: S3Addr,
      row: Row[A]
  ): Resource[IO, Iterator[A] => IO[Unit]] =
    //awsIO.tempWriter(output, row)
    awsIO.multiPartOutput(output, row)
}

abstract class DBS3App extends S3App {

  /**
    * this will be something like:
    * RDSTransactor.build(...).unsafeRunSync()(someDb)
    *
    * to cache the secret look up, make this a lazy val
    */
  def transactor: doobie.Transactor[IO]

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def dbSupportFactory: db.DBSupport.Factory

  lazy val dbInputFactory: InputFactory[IO] =
    dbSupportFactory.build(transactor)

  override def inputFactory[E[_]: Emittable, A](
      inputs: Iterable[(String, S3Addr)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): InputFactory[IO] =
    dbInputFactory.combine(super.inputFactory(inputs, e, blocker))
}

abstract class DBS3CliApp extends DBS3App {
  def eventArgs: Args

  def runIO(args: List[String]): IO[ExitCode] =
    IO.suspend {
      command.parse(args) match {
        case Right(cmd) =>
          implicit val ictx = contextShift
          cmd.run(eventArgs, blocker)
        case Left(err) =>
          IO {
            System.err.println(err)
            ExitCode.Error
          }
      }
    }

  final def main(args: Array[String]): Unit =
    System.exit(runIO(args.toList).unsafeRunSync().code)
}
