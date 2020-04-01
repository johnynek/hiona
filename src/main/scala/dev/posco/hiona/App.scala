package dev.posco.hiona

import cats.data.{Validated, ValidatedNel}
import cats.effect.{ContextShift, ExitCode, IO, IOApp}
import com.monovore.decline.{Argument, Command, Opts}
import java.nio.file.Path

import cats.implicits._

abstract class App[A: Row](results: Event[A]) extends IOApp {

  private val row: Row[A] = implicitly[Row[A]]

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      App.command.parse(args) match {
        case Right(cmd) => cmd.run(App.Args.EventArgs(row, results))
        case Left(err) =>
          IO {
            System.err.println(err)
            ExitCode.Error
          }
      }
    }
}

abstract class LabeledApp[K: Row, V: Row](results: LabeledEvent[K, V])
    extends IOApp {

  private val key: Row[K] = implicitly[Row[K]]
  private val value: Row[V] = implicitly[Row[V]]

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      App.command.parse(args) match {
        case Right(cmd) => cmd.run(App.Args.LabeledArgs(key, value, results))
        case Left(err) =>
          IO {
            System.err.println(err)
            ExitCode.Error
          }
      }
    }
}

object App {

  sealed abstract class Args
  object Args {
    case class EventArgs[A](row: Row[A], event: Event[A]) extends Args
    case class LabeledArgs[K, V](
        keyRow: Row[K],
        valueRow: Row[V],
        labeled: LabeledEvent[K, V]
    ) extends Args
  }

  implicit def named[A: Argument]: Argument[(String, A)] =
    new Argument[(String, A)] {
      val argA = implicitly[Argument[A]]
      val defaultMetavar = s"name=${argA.defaultMetavar}"
      def read(s: String): ValidatedNel[String, (String, A)] = {
        val splitIdx = s.indexOf('=')
        if (splitIdx < 0)
          Validated.invalidNel(
            s"string $s expected to have = character, not found"
          )
        else {
          val name = s.substring(0, splitIdx)
          val rest = s.substring(splitIdx + 1)
          argA.read(rest).map((name, _))
        }
      }
    }

  sealed abstract class Cmd {
    def run(args: Args)(implicit ctx: ContextShift[IO]): IO[ExitCode]
  }

  case class RunCmd(inputs: List[(String, Path)], output: Path) extends Cmd {
    val dupNames =
      inputs.groupBy(_._1).filter { case (_, res) => res.lengthCompare(1) > 0 }

    def run(args: Args)(implicit ctx: ContextShift[IO]): IO[ExitCode] =
      if (dupNames.isEmpty) {
        val io = args match {
          case Args.EventArgs(r, event) =>
            Engine.run(inputs.toMap, event, output)(r, ctx)
          case Args.LabeledArgs(k, v, l) =>
            Engine.runLabeled(inputs.toMap, l, output)(k, v, ctx)
        }
        io.map(_ => ExitCode.Success)
      } else {
        // this is an error
        IO {
          System.err.println("duplicated sources:")
          System.err.println(
            dupNames.toList.sortBy(_._1).mkString("\t", "\n", "")
          )
          System.err.flush()
          ExitCode.Error
        }
      }
  }

  private val runCmd: Command[RunCmd] =
    Command("run", "run an event and write all results to a csv file") {
      (
        Opts.options[(String, Path)]("input", "named path to CSV").orEmpty,
        Opts.option[Path]("output", "path to write")
      ).mapN(RunCmd(_, _))
    }

  case object ShowCmd extends Cmd {
    def run(args: Args)(implicit ctx: ContextShift[IO]): IO[ExitCode] = {
      val (srcs, lookups) =
        args match {
          case Args.EventArgs(_, event) =>
            (Event.sourcesOf(event).keys, Event.lookupsOf(event))
          case Args.LabeledArgs(_, _, lab) =>
            (
              LabeledEvent.sourcesAndOffsetsOf(lab).keys,
              LabeledEvent.lookupsOf(lab)
            )
        }
      IO {
        val names = srcs.toList.sorted.mkString("", ", ", "")
        println(s"sources: $names")

        println(s"lookups: ${lookups.size}")
        ExitCode.Success
      }
    }
  }

  private val showCmd: Command[ShowCmd.type] =
    Command("show", "print some details about the event to standard out") {
      Opts(ShowCmd)
    }

  val command: Command[Cmd] =
    Command("hiona", "feature engineering system") {
      Opts.subcommands(
        runCmd,
        showCmd
      )
    }

}
