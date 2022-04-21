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

package dev.posco.hiona.jobs

import cats.collections.Heap
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import cats.{Monoid, Order}
import com.monovore.decline.{Argument, Command, Opts}
import com.twitter.algebird.{Last, Max, Min, Moments}
import dev.posco.hiona.aws.{AWSIO, S3Addr}
import dev.posco.hiona.{Duration, Fs2Tools, Row, ShapelessMonoid, PipeCodec, Timestamp}
import fs2.{Pull, Stream}
import java.nio.file.Path

import cats.implicits._

/**
  * To run this:
  *
  * run:
  *   java -cp path/to/assembly.jar dev.posco.hiona.jobs.TradeStates
  *
  * this will print a help which gives the commands and required flags.
  *
  * As an example:
  *  aws s3 ls  --recursive  s3://predictionmachine-data/foresight/train_eval_sweeps/20200703T012002-585682 \
  *    | sed 's#.* #--input s3://predictionmachine-data/#' \
  *    | grep '.csv' \
  *    | xargs ./tradestats.sh balances --starting_balance 1500000 --threshold 0.8 --min_trade 1000
  *
  *  where ./tradestats.sh is just:
  *  #!/bin/bash
  *  java -cp jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar dev.posco.hiona.jobs.TradeStats "$@"
  */
object TradeStats extends IOApp {

  // case class StatRes(mean: Double, stddev: Double, skew: Double, kurt: Double)
  // object StatRes {
  //   def fromMom(m: Moments): StatRes =
  //     StatRes(mean = m.mean, stddev = m.stddev, skew = m.skewness, kurt = m.kurtosis)
  // }

  case class StatRes(mean: Double, stddev: Double)
  object StatRes {
    def fromMom(m: Moments): StatRes =
      StatRes(mean = m.mean, stddev = m.stddev)
  }

  /** a row of output summarizing statistics for a particular model scoreThreshold */
  case class Output(
      scoreThreshold: Double,
      minTs: Timestamp,
      maxTs: Timestamp,
      totalNumTrades: Long,
      trade: StatRes,
      tradeMax: Double,
      profit: StatRes,
      profitMin: Double,
      meanReturn: Double,
      twoSigmaDownsideReturn: Double
  )

  case class Stats(
      minDate: Min[Timestamp],
      maxDate: Max[Timestamp],
      count: Long,
      tradeMom: Moments,
      tradeMax: Max[Double],
      profitMom: Moments,
      profitMin: Min[Double]
  ) {

    def toOutput(threshold: Double): Output =
      Output(
        scoreThreshold = threshold,
        minTs = minDate.get,
        maxTs = maxDate.get,
        totalNumTrades = count,
        trade = StatRes.fromMom(tradeMom),
        tradeMax = tradeMax.get,
        profit = StatRes.fromMom(profitMom),
        profitMin = profitMin.get,
        meanReturn = profitMom.mean / tradeMom.mean,
        twoSigmaDownsideReturn =
          (profitMom.mean - 2.0 * profitMom.stddev) / tradeMom.mean
      )
  }

  object Stats {
    implicit val monoid: Monoid[Stats] = {
      import ShapelessMonoid._
      implicit val monMaxT: Monoid[Max[Timestamp]] =
        Max.monoid(Timestamp.MinValue)

      implicit val monMinT: Monoid[Min[Timestamp]] =
        Min.monoid(Timestamp.MaxValue)

      genericMonoid
    }
  }

  /**
    * This is the input describing type to read the CSV.
    *
    *  The name is meant to support versioning (maybe later we would make some files that had
    *  different fields).
    *
    * symbol,ts,dt_us_eastern,y_pred,gains,entry_turnover_usd,exit_turnover_usd,trade_usd,profit_usd
    * RYTM   1585842000000 2020-04-02 11:40:00-04:00  0.614486  0.012195              134974             450094      13497    164.603481
    */
  case class Trade0(
      symbol: String,
      ts: Timestamp,
      dtUsEastern: String,
      yPred: Double,
      gains: Double,
      entryTurnoverUsd: Double,
      exitTurnoverUsd: Double,
      tradeUsd: Double,
      profitUsd: Double
  ) {
    def toStats: Stats =
      Stats(
        Min(ts),
        Max(ts),
        1L,
        Moments(tradeUsd),
        Max(tradeUsd),
        Moments(profitUsd),
        Min(profitUsd)
      )
  }

  case class Balance(timestamp: Timestamp, amount: Double) {
    def add(amt: Double): Balance = Balance(timestamp, amount + amt)
  }
  sealed abstract class Transaction
  object Transaction {
    case class Enter(symbol: String, timestamp: Timestamp, amountUsd: Double)
        extends Transaction
    case class Exit(symbol: String, timestamp: Timestamp, amountUsd: Double)
        extends Transaction

    object Exit {
      implicit def ordExit: Order[Exit] =
        Order.by { exit: Exit => (exit.timestamp, exit.amountUsd, exit.symbol) }
    }
  }

  val HoldTimeMs: Duration = Duration.minutes(15)

  /**
    * This is a Pipe (a function from Stream to Stream) to compute a log of Transactions and Balances
    *
    * We start with an initial Balance and a minimum amount we would trade and build up a stream
    * that has both transactions (Enter/EXit events) as well as Balance events
    */
  def buildBalances(
      init: Balance,
      minUsd: Double
  ): fs2.Pipe[IO, Trade0, Either[Transaction, Balance]] = {
    import Transaction._

    def build(b: Balance, t: Trade0): (Balance, Option[(Enter, Exit)]) = {
      val thisAmt = t.tradeUsd min b.amount
      if (thisAmt < minUsd)
        // we can't enter:
        (b, None)
      else {
        // we need to schedule
        val frac = thisAmt / t.tradeUsd
        val credit = thisAmt + (frac * t.profitUsd)
        val enter = Enter(t.symbol, t.ts, thisAmt)
        val exit = Exit(t.symbol, t.ts + HoldTimeMs, credit)
        (Balance(t.ts, b.amount - thisAmt), Some((enter, exit)))
      }
    }

    def emitExits(
        ts: Timestamp,
        exits: Heap[Exit],
        acc: Double
    ): Pull[IO, Exit, (Double, Heap[Exit])] =
      exits.minimumOption match {
        case Some(min) if min.timestamp <= ts =>
          Pull.output1(min) >> emitExits(ts, exits.remove, acc + min.amountUsd)
        case _ => Pull.pure((acc, exits))
      }

    def loop(
        balance: Balance,
        exits: Heap[Exit],
        s: Stream[IO, Trade0]
    ): Pull[IO, Either[Transaction, Balance], Unit] =
      s.pull.uncons1
        .flatMap {
          case None =>
            def emptyExits(
                balance: Balance,
                exits: Heap[Exit]
            ): Pull[IO, Either[Transaction, Balance], Unit] =
              exits.pop match {
                case None => Pull.done
                case Some((exit, exits)) =>
                  val b1 = balance.add(exit.amountUsd)
                  Pull.output1(Right(b1)) >> emptyExits(b1, exits)
              }

            emptyExits(balance, exits)
          case Some((trade, rest)) =>
            val (b1, opt) = build(balance, trade)

            for {
              (inc, nextExits) <-
                emitExits(b1.timestamp, exits, 0.0).mapOutput(Left(_))
              b2 = b1.add(inc)
              _ <- Pull.output1(Right(b2))
              nextExits1 <- opt match {
                case None => Pull.pure(nextExits)
                case Some((enter, exit)) =>
                  Pull.output1(Left(enter)).as(nextExits.add(exit))
              }
              _ <- loop(b2, nextExits1, rest)
            } yield ()
        }

    { trades =>
      Stream(Right(init)) ++ loop(init, Heap.empty, trades).stream
    }
  }

  sealed abstract class Input {
    def getStream(blocker: Blocker): Stream[IO, Trade0]
  }

  object Input {
    sealed abstract class Input1 extends Input
    case class PathIn(path: Path) extends Input1 {
      def getStream(blocker: Blocker): Stream[IO, Trade0] = {
        implicit val codec = PipeCodec.csv[Trade0]()
        PipeCodec.stream[IO, Trade0](path, blocker)
      }
    }

    case class S3In(s3a: S3Addr) extends Input1 {
      def getStream(blocker: Blocker): Stream[IO, Trade0] =
        Stream
          .resource(AWSIO.awsS3)
          .flatMap { s3 =>
            val awsio = new AWSIO(s3)
            awsio
              .readCsv[IO, Trade0](s3a, 1 << 16, skipHeader = true, blocker)
          }
    }

    case class Many(inputs: NonEmptyList[Input1]) extends Input {
      def getStream(blocker: Blocker): Stream[IO, Trade0] = {
        val streams = inputs.map(_.getStream(blocker))
        implicit val ordTrade = Order.by { t: Trade0 => t.ts }
        Fs2Tools.sortMerge(streams.toList)
      }
    }

    implicit val inputArg: Argument[Input1] =
      new Argument[Input1] {
        val defaultMetavar = "input"

        val pa = Argument[Path]
        val s3arg = Argument[S3Addr]

        def read(arg: String): ValidatedNel[String, Input1] =
          s3arg.read(arg) match {
            case Validated.Valid(s3) => Validated.Valid(S3In(s3))
            case Validated.Invalid(errs) =>
              pa.read(arg) match {
                case Validated.Valid(p) => Validated.Valid(PathIn(p))
                case Validated.Invalid(errs2) =>
                  Validated.Invalid(errs ::: errs2)
              }
          }
      }

    val opts: Opts[Input] =
      Opts
        .options[Input1](
          "input",
          "path to local or s3 csv data (possibly gzipped)"
        )
        .map {
          case NonEmptyList(one, Nil) => one
          case many                   => Many(many)
        }
  }

  sealed abstract class Cmd
  case class ThreshSummary(
      in: Input,
      thresholds: NonEmptyList[Double]
  ) extends Cmd

  val opts: Opts[ThreshSummary] = {
    val ts = Opts
      .options[Double]("thresh", "a p(trade) threshold")
      .map(_.sorted)

    (Input.opts, ts).mapN(ThreshSummary(_, _))
  }

  case class Balances(
      input: Input,
      start: Double,
      minTrade: Double,
      threshold: Double
  ) extends Cmd
  val balOpts: Opts[Balances] =
    (
      Input.opts,
      Opts
        .option[Double]("starting_balance", "the amount in USD you start with"),
      Opts.option[Double]("min_trade", "the minimum trade in USD"),
      Opts.option[Double]("threshold", "the minimum trade prob")
    ).mapN(Balances(_, _, _, _))

  val command: Command[Cmd] =
    Command("tradestats", "process a list of trades into statistics") {
      Opts
        .subcommand("threshold", "compute returns at different thresholds")(
          opts
        )
        .orElse(
          Opts.subcommand(
            "balances",
            "show min, max balances and total profit/return"
          )(balOpts)
        )
    }

  def run(args: List[String]) =
    Blocker[IO].use { blocker =>
      command.parse(args) match {
        case Right(Balances(in, initBalance, minTrade, thresh)) =>
          // Here are all the input trades
          val strm: Stream[IO, Trade0] = in.getStream(blocker)

          // we are keeping only the ones at or above the threshold
          val good = strm.filter(t => t.yPred >= thresh)

          // this if the function that processes the stream of trades
          // into a stream of balance states.
          val fn =
            buildBalances(Balance(Timestamp.MinValue, initBalance), minTrade)

          good
            .through(fn)
            .collect {
              // the balance events are on the Right, the Enter/Exit are on the Left
              case Right(b) => b
            }
            .foldMap { b =>
              val amt = b.amount
              (Option(Last(amt)), Max(amt), Min(amt))
            }
            .compile
            .to(List)
            .flatMap {
              // since there is only a single element output of foldMap this
              // is the only possible result (unless the input stream never terminates)
              case (Some(Last(last)), Max(max), Min(min)) :: Nil =>
                IO {
                  val ret = last / initBalance
                  val biggestLoss = -(min / initBalance) + 1.0

                  println(
                    s"init = $initBalance, minTrade = $minTrade, thresh = $thresh, min = $min, max = $max, return = $ret, loss = $biggestLoss"
                  )
                  ExitCode.Success
                }
              case other =>
                sys.error(
                  s"found Nil, expected at least one balance: starting, $other"
                )
            }
        case Right(ThreshSummary(in, threshs)) =>
          val idxs = 0 until threshs.size
          val thresholds = threshs.toList.toArray

          def buildStats: fs2.Pipe[IO, Trade0, Map[Int, Stats]] =
            _.foldMap { t =>
              idxs.map { idx =>
                val stats =
                  if (t.yPred >= thresholds(idx)) t.toStats
                  else Stats.monoid.empty
                idx -> stats
              }.toMap
            }

          val results: IO[Map[Int, Stats]] =
            in.getStream(blocker)
              .prefetchN(10)
              .through(buildStats)
              .compile
              .to(List)
              .map(_.head)

          def showResults(map: Map[Int, Stats]): IO[Unit] = {
            // TODO: allow writing to file/s3
            val pw = IO(new java.io.PrintWriter(System.out, true))

            pw.flatMap { pw =>
              Row
                .writer[Output](pw)
                .flatMap { fn =>
                  val it = map.toList
                    .sortBy(_._1)
                    .map {
                      case (idx, stats) =>
                        val thresh = thresholds(idx)
                        stats.toOutput(thresh)
                    }
                    .iterator

                  fn(it)
                }
                .flatMap(_ => IO(pw.flush()))
            }
          }

          results.flatMap(showResults).as(ExitCode.Success)

        case Left(err) =>
          IO {
            System.err.println(err.toString)
            ExitCode.Error
          }
      }
    }

}
