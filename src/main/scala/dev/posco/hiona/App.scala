package dev.posco.hiona

import cats.data.{Validated, ValidatedNel}
import cats.effect.{ContextShift, IO, IOApp, ExitCode}
import com.monovore.decline.{Argument, Opts, Command}
import java.nio.file.Path

import Hiona.Event

import cats.implicits._

abstract class App[A: Row](results: Event[A]) extends IOApp {

  private val row: Row[A] = implicitly[Row[A]]

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      App.command.parse(args) match {
        case Right(cmd) => cmd.run(row, results)
        case Left(err) =>
          IO {
            System.err.println(err)
            ExitCode.Error
          }
      }
    }
}

object App {

  implicit def named[A: Argument]: Argument[(String, A)] =
    new Argument[(String, A)] {
      val argA = implicitly[Argument[A]]
      val defaultMetavar = s"name=${argA.defaultMetavar}"
      def read(s: String): ValidatedNel[String, (String, A)] = {
        val splitIdx = s.indexOf('=')
        if (splitIdx < 0) Validated.invalidNel(s"string $s expected to have = character, not found")
        else {
          val name = s.substring(0, splitIdx)
          val rest = s.substring(splitIdx + 1)
          argA.read(rest).map((name, _))
        }
      }
    }

  sealed abstract class Cmd {
    def run[A](r: Row[A], event: Event[A])(implicit ctx: ContextShift[IO]): IO[ExitCode]
  }

  case class RunCmd(inputs: List[(String, Path)], output: Path) extends Cmd {
    val dupNames = inputs.groupBy(_._1).filter { case (_, res) => res.lengthCompare(1) > 0 }

    def run[A](r: Row[A], event: Event[A])(implicit ctx: ContextShift[IO]): IO[ExitCode] =
      if (dupNames.isEmpty) {
        Engine.run(inputs.toMap, event, output)(r, ctx).map(_ => ExitCode.Success)
      }
      else {
        // this is an error
        IO {
          System.err.println("duplicated sources:")
          System.err.println(dupNames.toList.sortBy(_._1).mkString("\t", "\n", ""))
          System.err.flush()
          ExitCode.Error
        }
      }
  }

  private val runCmd: Command[RunCmd] =
    Command("run", "run an event and write all results to a csv file") {
      (Opts.options[(String, Path)]("input", "named path to CSV").orEmpty, Opts.option[Path]("output", "path to write"))
        .mapN(RunCmd(_, _))
    }

  val command: Command[Cmd] =
    Command("hiona", "feature engineering system") {
      Opts.subcommands(
        runCmd
      )
    }

}
