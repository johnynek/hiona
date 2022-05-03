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

package dev.posco.hiona

import cats.Semigroupal
import cats.arrow.FunctionK
import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource}
import com.monovore.decline.{Argument, Command, Opts}
import fs2.{Pipe, Pull, Stream}
import java.nio.file.Path

import cats.implicits._

class App[A](options: Opts[A], out: A => Output) extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.suspend {
      val cmdA = App.commandWith(App.command, options)
      IOEnv.read.flatMap { env =>
        cmdA.parse(args, env) match {
          case Right((cmd, a)) =>
            Blocker[IO].use(cmd.run(out(a), _))
          case Left(err) =>
            IO {
              System.err.println(err)
              ExitCode.Error
            }
        }
      }
    }
}

// This is an App that has no arguments
class App0(output: Output) extends App[Unit](Opts.unit, _ => output)

object App extends GenApp {
  type Ref = Path
  implicit def argumentForRef: Argument[Path] =
    Argument.readPath

  def inputFactory[E[_]: Emittable, A](
      inputs: Iterable[(String, Ref)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, InputFactory[IO]] =
    Resource.pure[IO, InputFactory[IO]](
      InputFactory.fromPaths(inputs, e, blocker)
    )

  def read[A](input: Ref, row: Row[A], blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): Stream[IO, A] = {
    implicit val ir = row
    Row.csvToStream[IO, A](input, skipHeader = true, blocker)
  }

  def sink[A](
      output: Path,
      row: Row[A]
  ): Pipe[IO, A, Nothing] =
    Fs2Tools.sinkStream(Row.writerRes[A](output)(row))
}

sealed abstract class Output {
  type Type
  val row: Row[Type]
  val value: Emittable.Value[Type]
}

object Output {
  def event[A](ev: Event[A])(implicit r: Row[A]): Output =
    new Output {
      type Type = A
      val row = r
      val value = Emittable.Value(ev)
    }

  def labeledEvent[A](ev: LabeledEvent[A])(implicit r: Row[A]): Output =
    new Output {
      type Type = A
      val row = r
      val value = Emittable.Value(ev)
    }
}

abstract class GenApp { self =>
  type Ref
  implicit def argumentForRef: Argument[Ref]

  def sink[A](output: Ref, row: Row[A]): Pipe[IO, A, Nothing]

  def read[A](input: Ref, row: Row[A], blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): Stream[IO, A]

  def inputFactory[E[_]: Emittable, A](
      inputs: Iterable[(String, Ref)],
      e: E[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, InputFactory[IO]]

  def pipe[A](implicit ctx: ContextShift[IO]): Pipe[IO, A, A] = { strm =>
    strm
      .chunkMin(1024)
      .flatMap(Stream.chunk(_))
      .prefetchN(1)
  }

  type StreamRes[+A] = Stream[IO, (Timestamp, A)]

  def run(
      args: Output,
      inputs: List[(String, Ref)],
      output: Ref,
      blocker: Blocker,
      onInput: Pipe[IO, Point, Point],
      onOutput: FunctionK[StreamRes, StreamRes]
  )(implicit
      ctx: ContextShift[IO]
  ): IO[Unit] =
    inputFactory(inputs, args.value, blocker)
      .use { input =>
        val row: Row[args.Type] = args.row

        val stream: Stream[IO, (Timestamp, args.Type)] =
          Engine.run(input.through(pipe).through(onInput), args.value)

        val result: Stream[IO, args.Type] =
          onOutput(stream.through(pipe)).map(_._2)

        result.through(sink(output, row)).compile.drain
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
    def run(args: Output, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
    ): IO[ExitCode]
  }

  case class RunCmd(
      inputs: List[(String, Ref)],
      output: Ref,
      logDelta: Option[Duration],
      limit: Option[Int],
      tb: TimeBound
  ) extends Cmd {
    def run(args: Output, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
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
                          if (
                            (point.offset eq Duration.Zero) && ordTs.lteq(
                              ts + dur,
                              thisTs
                            )
                          )
                            (thisTs, (counter, point) :: stack)
                          else prev
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
                                  vstr0.take(
                                    50
                                  ) + s"... (${vstr0.length - 50} more chars)"
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

      val limitFn = limit match {
        case Some(i) =>
          new FunctionK[StreamRes, StreamRes] {
            def apply[A](s: Stream[IO, (Timestamp, A)]) = s.take(i.toLong)
          }
        case None =>
          FunctionK.id[StreamRes]
      }

      // we can only filter lower bounded data at output
      val lowerBoundFn =
        new FunctionK[StreamRes, StreamRes] {
          def apply[A](s: Stream[IO, (Timestamp, A)]) =
            tb.filterOutput[IO, (Timestamp, A)](_._1)(s)
        }

      val inputFn = tb.filterInput[IO, Point](_.ts).andThen(loggerFn)
      val outputFn = lowerBoundFn.andThen(limitFn)
      self
        .run(args, inputs, output, blocker, inputFn, outputFn)
        .map(_ => ExitCode.Success)
    }
  }

  implicit val argDuration: Argument[Duration] =
    new Argument[Duration] {
      def defaultMetavar = "duration"
      def read(s: String): ValidatedNel[String, Duration] =
        try
          Validated.valid(
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
          .orNone,
        TimeBound.opts
      ).mapN(RunCmd(_, _, _, _, _))
    }

  case object ShowCmd extends Cmd {
    def run(args: Output, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
    ): IO[ExitCode] = {
      val cols = args.row.columnNames(0)
      val srcs = Emittable[Emittable.Value].sourcesAndOffsetsOf(args.value).keys
      val lookups = Emittable[Emittable.Value].toEither(args.value) match {
        case Right(ev) => Event.lookupsOf(ev)
        case Left(le)  => LabeledEvent.lookupsOf(le)
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
      else
        Left((m1.keySet -- m2.keySet, m2.keySet -- m1.keySet))

    def run(arg: Output, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
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
          srcMap <- pathMap(
            "event sources",
            Emittable[Emittable.Value]
              .sourcesAndOffsetsOf(arg.value)
              .toList
              .flatMap {
                case (k, (vs, _)) => vs.map((k, _))
              }
          )
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
        val resIF = inputFactory(List((ev.name, input)), e, blocker)

        resIF
          .use { infac =>
            val istream: fs2.Stream[IO, Point] =
              infac.allInputs(e)

            val points: IO[ArrayBuffer[Point]] =
              istream.chunks.compile
                .fold(ArrayBuffer[Point]()) { (buf, chunk) =>
                  buf ++= chunk.iterator; buf
                }
                .map(_.sortInPlace())

            for {
              ps <- points
              ait = ps.iterator.map {
                case Point.Sourced(_, a, _, _) => a.asInstanceOf[A]
              }
              aitStream = Stream.fromIterator[IO](ait)
              _ <- aitStream.through(sink(output, ev.row)).compile.drain
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

  case class DiffCmd(
      input1: Ref,
      input2: Ref,
      output: Option[Ref],
      limitIn: Option[Int],
      limit: Option[Int]
  ) extends Cmd {
    def run(args: Output, blocker: Blocker)(implicit
        ctx: ContextShift[IO]
    ): IO[ExitCode] = {

      val row: Row[args.Type] = args.row
      val s1: Stream[IO, args.Type] =
        read(input1, row, blocker).through(pipe)
      val s2: Stream[IO, args.Type] =
        read(input2, row, blocker).through(pipe)

      def toList[A](a: A, row: Row[A]): List[String] = {
        val ary = new Array[String](row.columns)
        row.writeToStrings(a, 0, ary)
        ary.toList
      }

      val both = s1.zip(s2)
      val bothLimit = limitIn.fold(both)(l => both.take(l.toLong)).zipWithIndex
      val diffs = bothLimit.filter { case ((a, b), _) => a != b }
      val diffLimit = limit.fold(diffs)(l => diffs.take(l.toLong))

      import com.softwaremill.diffx.{ConsoleColorConfig => CCC}
      implicit val cc = output match {
        case Some(_) =>
          // don't colorize
          CCC(identity, identity, identity, identity)
        case None =>
          CCC.default
      }

      val diffStrings = diffLimit.zipWithIndex.map {
        case (((a, b), idx), diffidx) =>
          import com.softwaremill.diffx._
          val aList = toList(a, row)
          val bList = toList(b, row)

          case class Col(column: Int, value: String)
          val diffIdx = aList
            .zip(bList)
            .zipWithIndex
            .collect { case ((a, b), col) if a != b => col }
            .toSet

          def toCol(ls: List[String]): List[Col] =
            ls.zipWithIndex
              .filter { case (_, idx) => diffIdx(idx) }
              .map { case (s, idx) => Col(idx, s) }

          val diffAB =
            compare(toCol(aList), toCol(bList)).show

          s"diff $diffidx from line $idx\n$diffAB\n"
      }

      val outPipe = output match {
        case Some(path) => sink(path, implicitly[Row[String]])
        case None =>
          stream: Stream[IO, String] => stream.lines(System.out).drain
      }

      outPipe(diffStrings).compile.drain.as(ExitCode.Success)
    }
  }

  private val diffCmd: Command[DiffCmd] =
    Command("diff", "compare two csv files and print any differences") {
      (
        Opts.argument[Ref]("input1"),
        Opts.argument[Ref]("input2"),
        Opts.option[Ref]("output", "path to write").orNone,
        Opts
          .option[Int]("limit_in", "maximum number of input rows to consider")
          .orNone,
        Opts
          .option[Int]("limit", "maximum number of events to write out")
          .orNone
      ).mapN(DiffCmd(_, _, _, _, _))
    }

  def commandWith[A, B](cmd: Command[A], opts: Opts[B]): Command[(A, B)] =
    // we don't need help because cmd already controls that.
    Command(cmd.name, cmd.header, helpFlag = false)(
      Semigroupal[Opts].product(cmd.options, opts)
    )

  val command: Command[Cmd] =
    Command("hiona", "feature engineering system") {
      Opts.subcommands(
        diffCmd,
        runCmd,
        showCmd,
        sortCmd
      )
    }

}
