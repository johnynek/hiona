package dev.posco.hiona

import cats.arrow.FunctionK
import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource}
import com.monovore.decline.{Argument, Command, Opts}
import fs2.{Pipe, Pull, Stream}
import java.nio.file.Path

import cats.implicits._

abstract class App[A: Row](results: Event[A]) extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      App.command.parse(args) match {
        case Right(cmd) =>
          Blocker[IO].use(cmd.run(Args.event(results), _))
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
          Blocker[IO].use(cmd.run(Args.labeledEvent(results), _))
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

  def inputFactory[E[_]: Engine.Emittable, A](
      inputs: Iterable[(String, Ref)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Engine.InputFactory[IO] =
    Engine.InputFactory.fromPaths(inputs, e, blocker)

  def writer[A](
      output: Path,
      row: Row[A]
  ): Resource[IO, Iterator[A] => IO[Unit]] =
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

  def writer[A](output: Ref, row: Row[A]): Resource[IO, Iterator[A] => IO[Unit]]

  def inputFactory[E[_]: Engine.Emittable, A](
      inputs: Iterable[(String, Ref)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Engine.InputFactory[IO]

  def pipe[A](implicit ctx: ContextShift[IO]): Pipe[IO, A, A] = { strm =>
    strm
      .chunkMin(1024)
      .flatMap(Stream.chunk(_))
      .prefetchN(10)
  }

  def run(
      args: Args,
      inputs: List[(String, Ref)],
      output: Ref,
      blocker: Blocker,
      onInput: Pipe[IO, Point, Point],
      onOutput: FunctionK[Stream[IO, *], Stream[IO, *]]
  )(
      implicit ctx: ContextShift[IO]
  ): IO[Unit] = {

    val (row, stream) = args match {
      case Args.EventArgs(r, event) =>
        val input: Engine.InputFactory[IO] =
          inputFactory(inputs, event, blocker)
        (r, Engine.run(input.through(pipe).through(onInput), event))
      case Args.LabeledArgs(r, le) =>
        val input: Engine.InputFactory[IO] =
          inputFactory(inputs, le, blocker)
        (r, Engine.run(input.through(pipe).through(onInput), le))
    }

    val writerRes = writer(output, row)

    val result = onOutput(stream.through(pipe))
    Fs2Tools.sinkStream(result, writerRes).compile.drain
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
    def run(args: Args, blocker: Blocker)(
        implicit ctx: ContextShift[IO]
    ): IO[ExitCode]
  }

  case class RunCmd(
      inputs: List[(String, Ref)],
      output: Ref,
      logDelta: Option[Duration],
      limit: Option[Int]
  ) extends Cmd {
    def run(args: Args, blocker: Blocker)(
        implicit ctx: ContextShift[IO]
    ): IO[ExitCode] = {

      val loggerFn: Pipe[IO, Point, Point] =
        logDelta match {
          case None => identity
          case Some(dur) =>
            def loop(
                ts: Timestamp,
                count: Long,
                points: Stream[IO, Point]
            ): Pull[IO, Point, Unit] =
              points.pull.uncons
                .flatMap {
                  case Some((points, next)) =>
                    val ordTs = Ordering[Timestamp]
                    var counter = count
                    val (nextTs, revPoints): (Timestamp, List[(Long, Point)]) =
                      points.foldLeft((ts, List.empty[(Long, Point)])) {
                        case (prev @ (ts, stack), point) =>
                          counter += 1L
                          val thisTs = point.ts
                          // only log real input events for now, not the shifted values
                          // (since it is hard to account for them
                          if ((point.offset eq Duration.Zero) && ordTs.lteq(
                                ts + dur,
                                thisTs
                              )) {
                            (thisTs, (counter, point) :: stack)
                          } else prev
                      }

                    val logMessage: IO[Unit] =
                      if (revPoints.isEmpty) IO.unit
                      else {
                        val pointsToLog = revPoints.reverse
                        IO {
                          pointsToLog.foreach {
                            case (count, point @ Point.Sourced(_, v, _, _)) =>
                              val vstr0 = v.toString
                              val vstr =
                                if (vstr0.length > 50)
                                  vstr0.take(50) + s"... (${vstr0.length - 50} more chars)"
                                else vstr0

                              println(
                                s"input event ($count) ts: ${point.ts.epochMillis}, from source: ${point.name}, value: $vstr"
                              )
                          }
                        }
                      }
                    val prints: Pull[IO, Point, Unit] = Pull.eval(logMessage)
                    prints *> Pull.output(points) *> loop(nextTs, counter, next)
                  case None => Pull.done
                }

            { points: Stream[IO, Point] =>
              loop(Timestamp.MinValue, -1L, points).stream
            }
        }

      val result = limit match {
        case Some(i) =>
          new FunctionK[Stream[IO, *], Stream[IO, *]] {
            def apply[A](s: Stream[IO, A]) = s.take(i)
          }
        case None =>
          FunctionK.id[Stream[IO, *]]
      }

      self
        .run(args, inputs, output, blocker, loggerFn, result)
        .map(_ => ExitCode.Success)
    }
  }

  implicit val argDuration: Argument[Duration] =
    new Argument[Duration] {
      def defaultMetavar = "duration"
      def read(s: String): ValidatedNel[String, Duration] =
        try Validated.valid(
          Duration(scala.concurrent.duration.Duration(s).toMillis)
        )
        catch {
          case (_: NumberFormatException) =>
            Validated.invalidNel(
              s"string $s could not be converted to a duration, try 1millis or 2hours or 3days"
            )
        }
    }

  private val runCmd: Command[RunCmd] =
    Command("run", "run an event and write all results to a csv file") {
      (
        Opts.options[(String, Ref)]("input", "named path to CSV").orEmpty,
        Opts.option[Ref]("output", "path to write"),
        Opts
          .option[Duration](
            "logevery",
            "interval of events between which to log"
          )
          .orNone,
        Opts
          .option[Int]("limit", "maximum number of events to write out")
          .orNone
      ).mapN(RunCmd(_, _, _, _))
    }

  case object ShowCmd extends Cmd {
    def run(args: Args, blocker: Blocker)(
        implicit ctx: ContextShift[IO]
    ): IO[ExitCode] = {
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
    def toMap[K, V](
        items: Iterable[(K, V)]
    ): Either[Iterable[(K, V)], Map[K, V]] = {
      val map = items.groupBy(_._1)
      val badKeys = map.filter {
        case (_, kvs) => kvs.iterator.take(2).size == 2
      }
      if (badKeys.isEmpty) Right(items.toMap)
      else {
        val badItems = items.filter { case (k, _) => badKeys.contains(k) }
        Left(badItems)
      }
    }

    def keyMatch[K, V1, V2](
        m1: Map[K, V1],
        m2: Map[K, V2]
    ): Either[(Set[K], Set[K]), Map[K, (V1, V2)]] =
      if (m1.keySet == m2.keySet) Right(m1.map {
        case (k, v1) => (k, (v1, m2(k)))
      })
      else {
        Left((m1.keySet -- m2.keySet, m2.keySet -- m1.keySet))
      }

    def run(arg: Args, blocker: Blocker)(
        implicit ctx: ContextShift[IO]
    ): IO[ExitCode] = {
      val data: IO[List[(String, ((Ref, Ref), Event.Source[_]))]] = {
        def pathMap[V](
            label: String,
            i: List[(String, V)]
        ): IO[Map[String, V]] =
          toMap(i) match {
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
          keyMatch(m1, m2) match {
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
      ): IO[Unit] = {
        import scala.collection.mutable.ArrayBuffer

        val e: Event[A] = ev
        val istream: fs2.Stream[IO, Point] =
          inputFactory(List((ev.name, input)), e, blocker).allInputs(e)

        val points: IO[ArrayBuffer[Point]] =
          istream.chunks.compile
            .fold(ArrayBuffer[Point]()) { (buf, chunk) =>
              buf ++= chunk.iterator; buf
            }
            .map(_.sortInPlace)

        writer(output, ev.row)
          .use { fn =>
            for {
              ps <- points
              ait = ps.iterator.map {
                case Point.Sourced(_, a, _, _) => a.asInstanceOf[A]
              }
              _ <- fn(ait.iterator)
            } yield ()
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
