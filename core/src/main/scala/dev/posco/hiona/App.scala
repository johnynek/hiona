package dev.posco.hiona

import cats.data.{Validated, ValidatedNel}
import cats.effect.{ContextShift, ExitCode, IO, IOApp, Resource}
import com.monovore.decline.{Argument, Command, Opts}
import java.nio.file.Path

import cats.implicits._

abstract class App[A: Row](results: Event[A]) extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      App.command.parse(args) match {
        case Right(cmd) => cmd.run(Args.event(results))
        case Left(err) =>
          IO {
            System.err.println(err)
            ExitCode.Error
          }
      }
    }
}

abstract class LabeledApp[A: Row](results: LabeledEvent[A]) extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      App.command.parse(args) match {
        case Right(cmd) =>
          cmd.run(Args.labeledEvent(results))
        case Left(err) =>
          IO {
            System.err.println(err)
            ExitCode.Error
          }
      }
    }
}

object App extends GenApp {
  type Ref = Path
  implicit def argumentForRef: Argument[Path] =
    Argument.readPath

  def run(args: Args, inputs: List[(String, Ref)], output: Ref)(
      implicit ctx: ContextShift[IO]
  ): IO[Unit] =
    args match {
      case Args.EventArgs(r, event) =>
        Engine.run(inputs, event, output)(r, ctx)
      case Args.LabeledArgs(r, l) =>
        Engine.runLabeled(inputs, l, output)(r, ctx)
    }

  def feeder[A](
      input: Path,
      ev: Event.Source[A],
      dur: Duration,
      strictTime: Boolean
  ): Resource[IO, Feeder] =
    Feeder.fromPath(input, ev, dur, strictTime)

  def writer[A](
      output: Path,
      row: Row[A]
  ): Resource[IO, Iterable[A] => IO[Unit]] =
    Row.writerRes[A](output)(row)
}

sealed abstract class Args {
  def sources: Map[String, Set[Event.Source[_]]]
}

object Args {
  def event[A](ev: Event[A])(implicit r: Row[A]): EventArgs[A] =
    EventArgs(r, ev)

  def labeledEvent[A](ev: LabeledEvent[A])(implicit r: Row[A]): LabeledArgs[A] =
    LabeledArgs(r, ev)

  final case class EventArgs[A](row: Row[A], event: Event[A]) extends Args {
    def columnNames: List[String] = row.columnNames(0)

    def sources: Map[String, Set[Event.Source[_]]] =
      Event.sourcesOf(event)
  }

  final case class LabeledArgs[A](
      row: Row[A],
      labeled: LabeledEvent[A]
  ) extends Args {
    def columnNames: List[String] =
      row.columnNames(0)
    def sources: Map[String, Set[Event.Source[_]]] =
      LabeledEvent.sourcesOf(labeled)
  }
}

abstract class GenApp { self =>
  type Ref
  implicit def argumentForRef: Argument[Ref]

  def run(args: Args, inputs: List[(String, Ref)], output: Ref)(
      implicit ctx: ContextShift[IO]
  ): IO[Unit]
  def feeder[A](
      input: Ref,
      ev: Event.Source[A],
      dur: Duration,
      strictTime: Boolean
  ): Resource[IO, Feeder]
  def writer[A](output: Ref, row: Row[A]): Resource[IO, Iterable[A] => IO[Unit]]

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

  case class RunCmd(inputs: List[(String, Ref)], output: Ref) extends Cmd {
    def run(args: Args)(implicit ctx: ContextShift[IO]): IO[ExitCode] =
      self.run(args, inputs, output).map(_ => ExitCode.Success)
  }

  private val runCmd: Command[RunCmd] =
    Command("run", "run an event and write all results to a csv file") {
      (
        Opts.options[(String, Ref)]("input", "named path to CSV").orEmpty,
        Opts.option[Ref]("output", "path to write")
      ).mapN(RunCmd(_, _))
    }

  case object ShowCmd extends Cmd {
    def run(args: Args)(implicit ctx: ContextShift[IO]): IO[ExitCode] = {
      val (cols, srcs, lookups) =
        args match {
          case a @ Args.EventArgs(_, event) =>
            (a.columnNames, Event.sourcesOf(event).keys, Event.lookupsOf(event))
          case la @ Args.LabeledArgs(_, lab) =>
            (
              la.columnNames,
              LabeledEvent.sourcesAndOffsetsOf(lab).keys,
              LabeledEvent.lookupsOf(lab)
            )
        }
      IO {
        val names = srcs.toList.sorted.mkString("", ", ", "")
        println(s"sources: $names")

        println(s"lookups: ${lookups.size}")
        println(s"output columns (${cols.size}): " + cols.mkString(", "))
        ExitCode.Success
      }
    }
  }

  private val showCmd: Command[ShowCmd.type] =
    Command("show", "print some details about the event to standard out") {
      Opts(ShowCmd)
    }

  case class SortCmd(
      inputs: List[(String, Ref)],
      outputs: List[(String, Ref)]
  ) extends Cmd {
    def run(arg: Args)(implicit ctx: ContextShift[IO]): IO[ExitCode] = {
      val data: IO[List[(String, ((Ref, Ref), Event.Source[_]))]] = {
        def pathMap[V](
            label: String,
            i: List[(String, V)]
        ): IO[Map[String, V]] =
          Feeder.toMap(i) match {
            case Right(m) => IO.pure(m)
            case Left(dups) =>
              IO.raiseError(new Exception(s"duplicates in $label: $dups"))
          }

        def tab[V1, V2](
            labelLeft: String,
            labelRight: String,
            m1: Map[String, V1],
            m2: Map[String, V2]
        ): IO[Map[String, (V1, V2)]] =
          Feeder.keyMatch(m1, m2) match {
            case Right(m) => IO.pure(m)
            case Left((lextra, rextra)) =>
              IO.raiseError(
                new Exception(
                  s"$labelLeft extras: ${lextra.toList.sorted}, $labelRight extras: ${rextra.toList.sorted}"
                )
              )
          }

        for {
          inMap <- pathMap("inputs", inputs)
          outMap <- pathMap("output", outputs)
          srcMap <- pathMap("event sources", arg.sources.toList.flatMap {
            case (k, vs) => vs.map((k, _))
          })
          table0 <- tab("inputs", "outputs", inMap, outMap)
          table <- table0.toList.traverse {
            case (k, pair) =>
              srcMap.get(k) match {
                case Some(e) => IO.pure((k, (pair, e)))
                case None =>
                  IO.raiseError(
                    new Exception(s"unknown source name: $k, not in the event")
                  )
              }
          }
        } yield table
      }

      // TODO: this could exhaust the memory, we could do an external sort
      def sortRef[A](
          input: Ref,
          output: Ref,
          ev: Event.Source[A]
      ): IO[Unit] =
        feeder(input, ev, Duration.zero, strictTime = false)
          .product(writer(output, ev.row))
          .use {
            case (feeder, writer) =>
              def allPoints(lines: Int, prev: List[Point]): IO[Array[Point]] =
                feeder
                  .nextBatch(100)
                  .flatMap {
                    case Nil => IO.pure(prev.reverse.toArray)
                    case nel =>
                      val newlines = lines + nel.size
                      val dbg =
                        if (newlines % 10000 == 0)
                          IO(println(s"read: $newlines"))
                        else IO.unit
                      dbg >> allPoints(lines + nel.size, nel reverse_::: prev)
                  }

              implicit val pointOrd: Ordering[Point] =
                new Ordering[Point] {
                  def compare(left: Point, right: Point) =
                    java.lang.Long
                      .compare(left.ts.epochMillis, right.ts.epochMillis)
                }

              allPoints(0, Nil)
                .flatMap(l => IO { println("sorting"); l })
                .map { ary =>
                  java.util.Arrays.sort(ary, pointOrd)
                  ary
                }
                .flatMap { sortPoints =>
                  val bldr = collection.mutable.Buffer.newBuilder[A]
                  bldr.sizeHint(sortPoints.length)
                  var idx = 0
                  while (idx < sortPoints.length) {
                    val Point.Sourced(_, a, _, _) = sortPoints(idx)
                    sortPoints(idx) = null
                    bldr += a.asInstanceOf[A]
                    idx += 1
                  }

                  IO(println("writing")) >> writer(bldr.result)
                }
          }

      for {
        d <- data
        _ <- d.parTraverse_ {
          case (_, ((in, out), ev)) => sortRef(in, out, ev)
        }
      } yield ExitCode.Success
    }
  }
  private val sortCmd: Command[SortCmd] =
    Command(
      "sort",
      "sort the inputs to make sure they are in time sorted order"
    ) {
      (
        Opts
          .options[(String, Ref)]("input", "named path to input CSV")
          .orEmpty,
        Opts
          .options[(String, Ref)]("output", "named path to target CSV")
          .orEmpty
      ).mapN(SortCmd(_, _))
    }

  val command: Command[Cmd] =
    Command("hiona", "feature engineering system") {
      Opts.subcommands(
        runCmd,
        showCmd,
        sortCmd
      )
    }

}
