package dev.posco.hiona.aws

import cats.data.{Validated, ValidatedNel}
import cats.effect.{ContextShift, IO, Resource}
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.monovore.decline.Argument
import java.io.{InputStream, OutputStream}
import java.nio.channels.Channels
import org.typelevel.jawn.ast
import scala.util.control.NonFatal
import com.amazonaws.services.s3

import dev.posco.hiona._

abstract class LambdaApp(appArgs: Args) extends RequestStreamHandler {

  // allocating this initializes s3clients
  private[this] lazy val s3App = new S3App

  def parseArgs(input: ast.JValue): Either[String, List[String]] = {
    val body = input.get("body")
    val args = body.get("args")
    args match {
      case ast.JArray(items) =>
        var idx = items.length
        var res: List[String] = Nil
        while (idx > 0) {
          idx = idx - 1
          items(idx) match {
            case ast.JString(s) =>
              res = s :: res
            case _ =>
              return Left(
                s"index position: $idx in ${items.toList} expected to be string"
              )
          }
        }
        Right(res)
      case other =>
        Left(s"expected JArray, found: $other")
    }
  }

  def handleRequest(
      inputStream: InputStream,
      outputStream: OutputStream,
      context: Context
  ): Unit = {
    val logger = context.getLogger()

    try {
      val inChannel = Channels.newChannel(inputStream)
      val jinput: ast.JValue = ast.JParser.parseFromChannel(inChannel).get

      parseArgs(jinput) match {
        case Right(stringArgs) =>
          logger.log(s"INFO: parsed input: $stringArgs")

          s3App.command.parse(stringArgs) match {
            case Right(cmd) =>
              implicit val ctx =
                IO.contextShift(scala.concurrent.ExecutionContext.global)
              cmd.run(appArgs).unsafeRunSync()
              ()
              val result = ast.JString("done")
              outputStream.write(result.render().getBytes("US-ASCII"))
              ()
            case Left(err) =>
              logger.log(s"ERROR: failed to parse command: $err")
          }
        case Left(msg) =>
          logger.log(
            s"ERROR: failed to parse input. $msg from: ${jinput.render()}"
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

  implicit def argumentForRef: Argument[S3Addr] =
    new Argument[S3Addr] {
      val defaultMetavar = "s3uri"
      def read(s: String): ValidatedNel[String, S3Addr] =
        if (!s.startsWith("s3://")) {
          Validated.invalidNel(
            s"string $s expected to start with s3:// character, not found"
          )
        } else {
          val tail = s.substring(5)
          val slash = tail.indexOf('/')
          if (slash < 0) {
            Validated.invalidNel(
              s"string $s expected to have / to separate bucket/key. not found"
            )
          } else {
            val bucket = tail.substring(0, slash)
            val key = tail.substring(slash + 1)
            Validated.valid(S3Addr(bucket, key))
          }
        }
    }

  def run(args: Args, inputs: List[(String, S3Addr)], output: S3Addr)(
      implicit ctx: ContextShift[IO]
  ): IO[Unit] =
    args match {
      case Args.EventArgs(r, event) =>
        val outRes = writer(output, r)
        val feederRes = Feeder.fromInputsFn(inputs, event) { (src, s3path) =>
          val is = awsIO.read(s3path)
          Feeder.fromInputStream(is, src, Duration.Zero)
        }
        Engine.Emitter
          .fromEvent(event)
          .flatMap(em => Engine.runEmitter(feederRes, em, outRes)(r, ctx))

      case Args.LabeledArgs(r, l) =>
        val outRes = writer(output, r)
        val feederRes = Feeder.fromInputsLabelsFn(inputs, l) {
          (src, s3path, dur) =>
            val is = awsIO.read(s3path)
            Feeder.fromInputStream(is, src, dur)
        }
        Engine.Emitter
          .fromLabeledEvent(l)
          .flatMap(em => Engine.runEmitter(feederRes, em, outRes)(r, ctx))
    }

  def feeder[A](
      input: S3Addr,
      ev: Event.Source[A],
      dur: Duration,
      strictTime: Boolean
  ): Resource[IO, Feeder] =
    Feeder.fromInputStream(awsIO.read(input), ev, dur, strictTime)

  def writer[A](
      output: S3Addr,
      row: Row[A]
  ): Resource[IO, Iterable[A] => IO[Unit]] =
    //awsIO.tempWriter(output, row)
    awsIO.multiPartOutput(output, row)
}
