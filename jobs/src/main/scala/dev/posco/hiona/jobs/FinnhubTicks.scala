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

import cats.Monoid
import cats.effect.{IO, Resource}
import com.twitter.algebird.{First, Last, Max, Min}
import dev.posco.hiona._

import cats.implicits._

import FinnhubDBCandle.Candle

/** Specify and evaluate a computation graph */
object FinnhubTicks {
  type Symbol = String

  case class Tick(
      symbol: Symbol,
      date: Row.Dummy,
      timestampMs: Long,
      volume: Long,
      priceMicroDollars: Long
  ) {

    def priceUsd: Double = priceMicroDollars.toDouble / 1e6
    def turnoverUsd: Double = priceUsd * volume
    val timestamp: Timestamp = Timestamp(timestampMs)
  }

  val candleWidth: Duration = Duration.minutes(5)

  case class CandleWindow[Width <: Duration](
      open: Option[First[Double]],
      close: Option[Last[Double]],
      high: Max[Double],
      low: Min[Double],
      volume: Long,
      turnoverUsd: Double,
      trades: Long,
      endTimestamp: Max[Timestamp]
  ) {

    def change: Double =
      (close, open)
        .mapN(_.get / _.get)
        .getOrElse(Double.NaN)

    def priceRange: Double =
      high.get / low.get

    def volumeWeightedPrice: Double =
      turnoverUsd / volume

    def toCandle(sym: Symbol, width: Duration): Candle =
      Candle(
        symbol = sym,
        candleStartEpochMillis = (endTimestamp.get - width).epochMillis,
        candleEndEpochMillis = endTimestamp.get.epochMillis,
        open = open.fold(java.lang.Double.NaN)(_.get),
        high = high.get,
        low = low.get,
        close = close.fold(java.lang.Double.NaN)(_.get),
        volume = volume.toDouble,
        exchCode = "US",
        currency = "USD"
      )
  }

  object CandleWindow {
    implicit def monoid[W <: Duration]: Monoid[CandleWindow[W]] = {
      import ShapelessMonoid._
      implicit val maxTs: Monoid[Max[Timestamp]] =
        Max.monoid(Timestamp.MinValue)
      genericMonoid
    }

    def fromTick[W <: Duration](t: Tick): CandleWindow[W] = {
      val price = t.priceUsd
      CandleWindow(
        Some(First(price)),
        Some(Last(price)),
        Max(price),
        Min(price),
        t.volume,
        t.turnoverUsd,
        1L,
        Max(t.timestamp)
      )
    }

    implicit def ordering[W <: Duration]: Ordering[CandleWindow[W]] =
      Ordering.by { cw: CandleWindow[W] => (cw.endTimestamp, cw.volume) }
  }

  val src: Event.Source[Tick] = Event.source[Tick](
    "finnhub.stock_ticks",
    Validator.pure[Tick](_.timestamp)
  )

  val bySymbol: Event[(Symbol, Tick)] = src.map(t => (t.symbol, t))

  val fiveMinCandle: Feature[Symbol, CandleWindow[candleWidth.type]] =
    bySymbol
      .mapValues(CandleWindow.fromTick[candleWidth.type])
      .windowSum(candleWidth)

  val candleSrc: Event[Candle] =
    bySymbol
      .preLookup(fiveMinCandle)
      .postLookup(fiveMinCandle)
      .concatMap {
        case (_, ((tick, cwPre), cwPost)) =>
          /*
           * If we have crossed into the new bucket, we emit, which is to say
           * we emit only the first Event in each candleWidth size bucket
           */
          val crossedBoundary =
            (cwPre.endTimestamp.get / candleWidth) != (cwPost.endTimestamp.get / candleWidth)

          if (crossedBoundary) cwPost.toCandle(tick.symbol, candleWidth) :: Nil
          else Nil
      }

  val result =
    new FinnhubDBCandle.ResultModule(candleSrc, Featurize.mockExchange)

  val eventOutput = Output.labeledEvent(result.labeled)
}

object FinnhubPathTicks extends App0(FinnhubTicks.eventOutput)

object FinnhubDBTicks extends aws.DBS3CliApp {

  def eventOutput = FinnhubTicks.eventOutput

  def transactor: Resource[IO, doobie.Transactor[IO]] =
    Resource.liftF(
      Databases
        .pmdbProdTransactor[IO]
        .apply(blocker, contextShift)
    )

  def dbSupportFactory: IO[db.DBSupport.Factory] = {
    import com.monovore.decline.{Command, Opts}

    // get from env vars, which are set in payload
    val dbCmd = Command("FinnhubTicks", "FinnhubTicks env args") {
      (
        Opts
          .env[Long](
            "inclusive_lower_ms",
            "inclusive lower bound on ts"
          ),
        Opts
          .env[Long](
            "exclusive_upper_ms",
            "exclusive upper bound on ts"
          )
      ).tupled
    }

    IOEnv
      .readArgs(dbCmd)
      .map {
        case (min, max) =>
          db.DBSupport.factoryFor(FinnhubTicks.src) {
            import doobie.implicits._

            sql"""SELECT _symbol, timestamp_ms, volume, price_e6
                FROM finnhub.stock_ticks
                WHERE timestamp_ms < $max AND timestamp_ms >= $min
                ORDER BY timestamp_ms """
              .query[(String, Long, Long, Long)]
              .map {
                case (s, ts, v, p) => FinnhubTicks.Tick(s, Row.Dummy, ts, v, p)
              }
          }
      }
  }
}
