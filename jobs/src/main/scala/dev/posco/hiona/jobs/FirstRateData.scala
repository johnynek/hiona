package dev.posco.hiona.jobs

import cats.ApplicativeError
import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource}
import dev.posco.hiona.aws.{AWSIO, S3Addr}
import dev.posco.hiona.{Duration, Fs2Tools, Row, Timestamp}
import fs2.{Pull, Stream}
import java.nio.file.Path
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.zip.{ZipEntry, ZipFile}
import java.util.regex.Pattern
import org.slf4j.LoggerFactory
import scala.util.Try
import com.monovore.decline.{Command, Opts}

import cats.implicits._

object FirstRateData {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private def openZipFileInternal(
      path: Path,
      names: Option[List[String]],
      chunkSize: Int,
      blocker: Blocker
  )(implicit
      ctx: ContextShift[IO]
  ): Resource[IO, List[(String, Stream[IO, Byte])]] =
    Resource
      .make(IO(new ZipFile(path.toFile)))(zf => IO(zf.close()))
      .flatMap { zipFile =>
        def streamFromEntry(ze: ZipEntry): (String, Stream[IO, Byte]) =
          (
            ze.getName,
            fs2.io.readInputStream(
              IO(zipFile.getInputStream(ze)),
              chunkSize,
              blocker
            )
          )

        def entries: IO[List[ZipEntry]] =
          IO.suspend {
            val ents = zipFile.entries()

            def nextOpt(): Option[ZipEntry] =
              if (ents.hasMoreElements()) Some(ents.nextElement())
              else None

            def loop(acc: List[ZipEntry]): IO[List[ZipEntry]] =
              IO(nextOpt()).flatMap {
                case None    => IO.pure(acc.reverse)
                case Some(a) => loop(a :: acc)
              }

            loop(Nil)
          }

        Resource.liftF(names match {
          case None => entries.map(_.map(streamFromEntry))
          case Some(nms) =>
            IO(
              nms
                .map { nm =>
                  val ze = zipFile.getEntry(nm)
                  if (ze ne null) streamFromEntry(ze)
                  else
                    (
                      nm,
                      Stream.eval(
                        IO.raiseError(
                          new IllegalArgumentException(
                            s"no element named: $nm found in $path"
                          )
                        )
                      )
                    )
                }
            )
        })
      }

  def openZipFile(path: Path, chunkSize: Int, blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): Resource[IO, List[(String, Stream[IO, Byte])]] =
    openZipFileInternal(path, None, chunkSize, blocker)

  def openZipFileNames(
      path: Path,
      names: List[String],
      chunkSize: Int,
      blocker: Blocker
  )(implicit
      ctx: ContextShift[IO]
  ): Resource[IO, List[(String, Stream[IO, Byte])]] =
    openZipFileInternal(path, Some(names), chunkSize, blocker)

  private[this] val tz: TimeZone = TimeZone.getTimeZone("America/New York")

  /**
    * The format the dates are written in
    */
  def format: () => SimpleDateFormat =
    Timestamp.Formats.dashedSpace8601(tz)

  val dateParser: String => Try[Timestamp] =
    Timestamp.parser(format)

  //  {DateTime, Open, High, Low, Close, Volume}
  case class Input(
      dateTime: String,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Long
  ) {

    lazy val candleStartTimestamp: Timestamp =
      dateParser(dateTime).get

    def candleEndTimestamp: Timestamp = candleStartTimestamp + Duration.minute
  }

  case class Output(
      symbol: String,
      candleStartInclusiveMs: Timestamp,
      candleEndExclusiveMs: Timestamp,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Long
  )

  object Output {
    def apply(symbol: String, input: Input): Output =
      Output(
        symbol = symbol,
        candleStartInclusiveMs = input.candleStartTimestamp,
        candleEndExclusiveMs = input.candleEndTimestamp,
        open = input.open,
        high = input.high,
        low = input.low,
        close = input.close,
        volume = input.volume
      )
  }

  def parseInput[F[_]](implicit
      ae: ApplicativeError[F, Throwable]
  ): fs2.Pipe[F, Byte, Input] =
    fs2.text.utf8Decode.andThen(
      Row
        .decodeFromCSV[F, Input](implicitly[Row[Input]], skipHeader = false)
    )

  // we have to declare which symbol we are dealing with
  def parseOutput[F[_]](
      symbol: String
  )(implicit ae: ApplicativeError[F, Throwable]): fs2.Pipe[F, Byte, Output] =
    parseInput[F].andThen(_.map(Output(symbol, _)))

  def sortMerge(
      path: Path,
      names: List[(String, String)],
      chunkSize: Int,
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Stream[IO, Output] =
    Stream
      .resource(openZipFileNames(path, names.map(_._1), chunkSize, blocker))
      .flatMap { list =>
        val nameMap = names.toMap
        val rlist: List[Stream[IO, Output]] =
          list.map {
            case (name, bytes) =>
              bytes.through(parseOutput(nameMap(name)))
          }

        implicit val orderingRec = Ordering.by { r: Output =>
          (r.candleStartInclusiveMs, r.symbol)
        }
        Fs2Tools.sortMerge(rlist)
      }

  // List(symbol, yyyy, qq) in UTC
  def toPathParts: (String, Timestamp) => List[String] = {
    val fmt = new ThreadLocal[(SimpleDateFormat, SimpleDateFormat)] {
      override def initialValue = {
        val utc = TimeZone.getTimeZone("UTC")

        val y = new SimpleDateFormat("yyyy")
        y.setTimeZone(utc)

        val m = new SimpleDateFormat("MM")
        m.setTimeZone(utc)

        (y, m)
      }
    }

    { (sym: String, ts: Timestamp) =>
      val (y, m) = fmt.get
      val d = new Date(ts.epochMillis)

      sym :: y.format(d) :: m.format(d) :: Nil
    }
  }

  trait Writer[-A] {
    def write(item: A): IO[Boolean]
    def close: IO[Unit]
  }

  object Writer {
    def filtered[A](
        writeRes: Resource[IO, Iterator[A] => IO[Unit]]
    )(fn: A => Boolean): IO[Writer[A]] =
      writeRes.allocated
        .map {
          case (writeFn, closeRes) =>
            new Writer[A] {
              def write(item: A) =
                if (fn(item)) writeFn(Iterator.single(item)).as(true)
                else IO.pure(false)

              def close = closeRes
            }
        }

    def recordPartioned(
        expectedParts: List[String],
        res: Resource[IO, Iterator[Output] => IO[Unit]]
    ): IO[Writer[Output]] =
      filtered(res) { rec =>
        toPathParts(rec.symbol, rec.candleEndExclusiveMs) == expectedParts
      }
  }

  trait Partitioner[A] {
    def next(a: A): IO[Writer[A]]
    def close: IO[Unit]

    final def pipe: fs2.Pipe[IO, A, Unit] = { input: Stream[IO, A] =>
      def run(
          input: Stream[IO, A],
          writer: Option[Writer[A]]
      ): Pull[IO, Unit, Unit] =
        input.pull.uncons
          .flatMap {
            case Some((chunk, tail)) =>
              def writeNew(a: A): IO[Option[Writer[A]]] =
                for {
                  writer <- next(a)
                  good <- writer.write(a).onError(_ => close)
                  _ <-
                    if (!good)
                      (writer.close *> IO.raiseError(
                        new IllegalStateException(
                          s"item: $a built $writer which would not accept the value"
                        )
                      ))
                    else IO.unit
                } yield Some(writer)

              val writeChunk = chunk.foldM(writer) {
                case (None, a) => writeNew(a)
                case (s @ Some(w), a) =>
                  w.write(a)
                    .onError(_ => w.close)
                    .flatMap {
                      case true  => IO.pure(s)
                      case false => w.close *> writeNew(a)
                    }
              }

              for {
                nextW <- Pull.eval(writeChunk)
                _ <- run(tail, nextW)
              } yield ()
            case None =>
              writer.fold(Pull.done.covary[IO]) { w =>
                Pull.eval(w.close) >> Pull.done
              }
          }

      run(input, None).stream
    }
  }

  object Partitioner {
    // put items in base / symbol / yyyy / qq / data.csv.gz
    def frdPart(
        partRes: List[String] => Resource[IO, Iterator[Output] => IO[Unit]]
    ): Partitioner[Output] =
      new Partitioner[Output] {
        def next(a: Output): IO[Writer[Output]] = {
          val part = toPathParts(a.symbol, a.candleEndExclusiveMs)

          Writer.recordPartioned(part, partRes(part))
        }

        def close: IO[Unit] = IO.unit
      }

    // the base directory the full key prefix, something like
    // s3://data-pm/sources/processed/firstratedata_us1500/
    def aws(base: S3Addr, awsIO: AWSIO): Partitioner[Output] = {
      val partRes = { parts: List[String] =>
        val fullKey = parts.foldLeft(base)(_ / _) / "candle1min.csv.gz"

        awsIO.multiPartOutput(fullKey, implicitly[Row[Output]])
      }

      frdPart(partRes)
    }

    def localFs(base: Path): Partitioner[Output] = {
      val partRes = { parts: List[String] =>
        val fullPath =
          parts.foldLeft(base)(_.resolve(_)).resolve("candle1min.csv.gz")

        Row.writerRes[Output](fullPath)
      }

      frdPart(partRes)
    }
  }

  /**
    * Parse out the symbol from file names that look like:
    * ticker_D_F/DELL_2000_2009.txt
    */
  def symbolFromPath: (Path, String) => IO[Option[(String, Int)]] = {
    val year = "(20\\d\\d)"
    val pat = Pattern.compile("[^/]+/([A-Z]+)_" + year + "_" + year + "\\.txt$")

    { (_, str) =>
      IO {
        val m = pat.matcher(str)
        if (m.matches) Some((m.group(1), m.group(2).toInt))
        else None
      }
    }
  }

  def partitionZips[K: Ordering, A](
      paths: List[Path],
      part: Partitioner[A],
      pathToKey: (Path, String) => IO[Option[K]],
      decoder: K => fs2.Pipe[IO, Byte, A],
      chunkSize: Int,
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    paths
      .traverse(path => openZipFile(path, chunkSize, blocker).map((path, _)))
      .use { streams: List[(Path, List[(String, Stream[IO, Byte])])] =>
        logger.info(s"partitionedZips opened all paths: $paths")

        val allItems = for {
          (path, parts) <- streams
          (item, strm) <- parts
          log = Stream.eval_(IO(logger.info(s"opening $path item: $item")))
        } yield (path, item, log ++ strm)

        val sortKeyed: IO[List[(K, Stream[IO, Byte])]] =
          allItems
            .traverse {
              case (path, item, strm) =>
                pathToKey(path, item).map {
                  case Some(k) => (k, strm) :: Nil
                  case None    => Nil
                }
            }
            .map(_.flatten.sortBy(_._1))

        val write: IO[Unit] =
          sortKeyed.flatMap { list =>
            Stream
              .emits(list)
              // we are in sorted order (so we just combine the pipes)
              .flatMap {
                case (k, bytes) =>
                  Stream.eval_(IO(logger.info(s"partitioning key: $k"))) ++
                    bytes
                      .through(decoder(k))
              }
              .prefetchN(1)
              .through(part.pipe)
              .compile
              .drain
          }

        write
      }

  def partitionOutputsAws(
      paths: List[Path],
      base: S3Addr,
      aws: AWSIO,
      chunkSize: Int,
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    partitionZips(
      paths,
      Partitioner.aws(base, aws),
      symbolFromPath,
      { symYear: (String, Int) => parseOutput[IO](symYear._1) },
      chunkSize,
      blocker
    )

  def partitionOutputsLocal(
      paths: List[Path],
      base: Path,
      chunkSize: Int,
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    partitionZips(
      paths,
      Partitioner.localFs(base),
      symbolFromPath,
      { symYear: (String, Int) => parseOutput[IO](symYear._1) },
      chunkSize,
      blocker
    )
}

object FirstRateDataApp extends IOApp {
  type Cmd = Command[Blocker => IO[ExitCode]]

  private val list =
    Opts.subcommand("list", "list items in a path to a zip") {
      Opts
        .arguments[Path]("files to list")
        .map { paths => blocker: Blocker =>
          paths
            .traverse_ { path =>
              IO(System.err.println(s"in: $path")) *>
                FirstRateData
                  .openZipFile(path, 1 << 16, blocker)
                  .use(_.traverse_ { case (name, _) => IO(println(name)) })
            }
            .as(ExitCode.Success)
        }
    }

  private val cat =
    Opts.subcommand("cat", "echo part of a zip file") {
      (
        Opts.option[Path]("path", "files to cat"),
        Opts.options[String]("part", "a part inside")
      ).mapN { (path, parts) => blocker: Blocker =>
        FirstRateData
          .openZipFileNames(path, parts.toList, 1 << 16, blocker)
          .use { items =>
            Stream
              .emits(items)
              .flatMap(_._2.through(fs2.io.stdout(blocker)))
              .compile
              .drain
          }
          .as(ExitCode.Success)
      }
    }

  private val parse =
    Opts.subcommand("parse", "parse part of a zip file") {
      (
        Opts.option[Path]("path", "files to cat"),
        Opts.option[String]("symbol", "a symbol inside"),
        Opts
          .option[Int]("chunk_size", "blocksize to read (default 32KB)")
          .withDefault(1 << 16)
      ).mapN { (path, symbol, chunkSize) => blocker: Blocker =>
        FirstRateData
          .openZipFile(path, chunkSize, blocker)
          .use { items =>
            Stream
              .emits(items)
              .flatMap {
                case (part, strm) =>
                  Stream
                    .eval(FirstRateData.symbolFromPath(path, part))
                    .flatMap {
                      case Some((sym, _)) if sym == symbol =>
                        strm.through(FirstRateData.parseOutput[IO](sym))
                      case _ => Stream.empty
                    }
              }
              .evalMap { rec =>
                IO(println(rec.toString))
              }
              .compile
              .drain
          }
          .as(ExitCode.Success)
      }
    }

  private def fetchS3(
      s3s: NonEmptyList[S3Addr],
      aio: AWSIO,
      blocker: Blocker
  ): Resource[IO, NonEmptyList[Path]] =
    for {
      temps <- s3s.traverse(uri =>
        Row.tempPath("s3_firstratedata", ".zip").map((uri, _))
      )
      _ <- temps.traverse {
        case (s3, path) =>
          Resource.liftF(aio.download(s3, path, blocker))
      }
    } yield temps.map(_._2)

  private val migrate =
    Opts.subcommand("migrate", "shard files into symbol/yyyy/mm format") {
      (
        Opts.options[Path]("input", "a zip file to read").orEmpty,
        Opts.options[S3Addr]("s3in", "an s3 uri to a zip to read").orEmpty,
        Opts
          .option[Path]("base", "output base to write into")
          .map(Left(_))
          .orElse(
            Opts
              .option[S3Addr]("base_s3", "output base to write into on s3")
              .map(Right(_))
          ),
        Opts
          .option[Int]("chunk_size", "blocksize to read (default 32KB)")
          .withDefault(1 << 16)
      ).mapN {
        case (paths, s3s, Left(base), chunkSize) =>
          blocker: Blocker =>
            val fromS3Res = NonEmptyList.fromList(s3s) match {
              case None => Resource.pure[IO, List[Path]](Nil)
              case Some(nel) =>
                AWSIO.resource
                  .flatMap(fetchS3(nel, _, blocker))
                  .map(_.toList)
            }

            fromS3Res
              .use { temps =>
                FirstRateData
                  .partitionOutputsLocal(
                    paths ::: temps,
                    base,
                    chunkSize,
                    blocker
                  )
                  .as(ExitCode.Success)
              }
        case (paths, s3s, Right(base), chunkSize) =>
          blocker: Blocker =>
            val aioPaths =
              AWSIO.resource.flatMap { aio =>
                val pathsRes = NonEmptyList.fromList(s3s) match {
                  case None =>
                    Resource.pure[IO, List[Path]](Nil)
                  case Some(nel) =>
                    fetchS3(nel, aio, blocker).map(_.toList)
                }

                pathsRes.map((aio, _))
              }

            aioPaths
              .use {
                case (aio, temps) =>
                  FirstRateData
                    .partitionOutputsAws(
                      paths ::: temps,
                      base,
                      aio,
                      chunkSize,
                      blocker
                    )
                    .as(ExitCode.Success)
              }
      }
    }

  val cmd: Command[Blocker => IO[ExitCode]] =
    Command("firstrate", "partition first rate data") {
      list
        .orElse(cat)
        .orElse(migrate)
        .orElse(parse)
    }

  def run(args: List[String]) =
    cmd.parse(args) match {
      case Right(fn) => Blocker[IO].use(fn)
      case Left(err) =>
        IO {
          System.err.println(err.toString)
          ExitCode.Error
        }
    }
}
