package dev.posco.hiona.jobs

import cats.Monoid
import cats.effect.{Async, IO}
import cats.implicits._
import dev.posco.hiona._
import dev.posco.hiona.db.DBSupport
import doobie.implicits._

object FinnhubDBCandle1 extends aws.DBS3CliApp {

  type Symbol = String
  type ExchangeCode = String
  type Currency = String

  // TODO: pass from payload
  val exch_code: ExchangeCode = "HK"
  val sqlQueryLimit: Int = 1000

  // TODO: refactor for compile speed
  // TODO: adjust lookforward amounts for labels
  // TODO: refactor so can make a job for Ticks
  // TODO: featurize volume spikes on declines, featurize price digits
  // TODO: is candle_end_epoch_millis too early for "overnight candles"?? should it be checked against next start instead?

  /**
    * This describes the form of the input data. In this case, mirrors
    * the DB table from which the data comes
    */
  case class Candle(
      symbol: Symbol,
      candle_start_epoch_millis: Long,
      candle_end_epoch_millis: Long,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Double,
      exch_code: ExchangeCode,
      currency: Currency
  ) {
    def change: Double = close / open
    def priceRange: Double = high / low
  }

  // so usdInLocal for HKD is ~7.75
  case class CurrencyExchange(
      base_currency: Currency,
      timestamp_epoch_millis: Long,
      one_base_in_quote: Double
  ) {
    def toUSD(localAmt: Double): Double = localAmt / one_base_in_quote
  }

  val valueInUSD: Event.Source[CurrencyExchange] =
    Event.source[CurrencyExchange](
      "finnhub.exchange_rates",
      Validator.pure(ce => Timestamp(ce.timestamp_epoch_millis))
    )

  val latestExchange: Feature[Currency, Option[CurrencyExchange]] =
    valueInUSD
      .map(ce => (ce.base_currency, ce))
      .latest(Duration.Infinite)
  // Feature.const(Option(CurrencyExchange("HKD", 7.75, Long.MinValue)))

  val src: Event.Source[Candle] = Event.source[Candle](
    "finnhub.stock_candles",
    Validator.pure(candle => Timestamp(candle.candle_end_epoch_millis))
  )

  val latestCandle: Feature[Symbol, Option[Candle]] =
    src
      .map(candle => (candle.symbol, candle))
      .latest(Duration.Infinite)

  case class Target(close: Double, volume: Double)

  val label: Label[Symbol, Target] = Label(latestCandle.map {
    case None    => Target(Double.NaN, 0L)
    case Some(c) => Target(c.close, c.volume)
  })

  /**
    * Our featurization consists of "zero history" features, some labels (as in
    * labels for supervised learning), and "data cubed" features. The values
    * below are what is subjected to the "data cubing" -- for each of these,
    * various transforms will be computed
    *
    * TODO: rename Values -> ValuesToFeaturize? FeaturizationInputs?
    */
  case class Values[A](close: A, change: A, range: A, volume: A) {

    def map[B](fn: A => B): Values[B] =
      Values(
        close = fn(this.close),
        change = fn(this.change),
        range = fn(this.range),
        volume = fn(this.volume)
      )

    def zip[B](that: Values[B]): Values[(A, B)] =
      Values(
        close = (this.close, that.close),
        change = (this.change, that.change),
        range = (this.range, that.range),
        volume = (this.volume, that.volume)
      )
  }

  object Values {
    implicit def monoid[A: Monoid]: Monoid[Values[A]] = {
      import ShapelessMonoid._

      genericMonoid
    }

    implicit def doubleModule[A: DoubleModule]: DoubleModule[Values[A]] =
      DoubleModule.genericModule

    def fromCandle(c: Candle): Values[Double] =
      Values(c.close, c.change, c.priceRange, c.volume)

    def moments(vd: Values[Double]): Values[Moments2] = {
      import Moments2.value

      Values(
        close = value(vd.close),
        change = value(vd.change),
        range = value(vd.range),
        volume = value(vd.volume)
      )
    }
  }

  /**
    * A Decay computes an exponentially decaying function of its
    * history of inputs. `Decays` enumerates several half-lives.
    * Together, they capture more information about the data, emphasizing
    * recency
    */
  case class Decays[A](
      mins10: A,
      hours1: A,
      hours2: A,
      days1: A,
      days3: A,
      days14: A
  ) {
    def map[B](fn: A => B): Decays[B] =
      Decays(
        mins10 = fn(mins10),
        hours1 = fn(hours1),
        hours2 = fn(hours2),
        days1 = fn(days1),
        days3 = fn(days3),
        days14 = fn(days14)
      )
  }

  object Decays {
    val mins10: Duration = Duration.minute * 10
    val hours1: Duration = Duration.hour
    val hours2: Duration = Duration.hour * 2
    val days1: Duration = Duration.day
    val days3: Duration = Duration.day * 3
    val days14: Duration = Duration.day * 14

    def feature[K, V: DoubleModule](
        ev: Event[(K, V)]
    ): Feature[K, Decays[V]] = {
      val tupled = ev.valueWithTime
        .mapValues {
          case (v, ts) =>
            (
              Decay.fromTimestamped[mins10.type, V](ts, v),
              Decay.fromTimestamped[hours1.type, V](ts, v),
              Decay.fromTimestamped[hours2.type, V](ts, v),
              Decay.fromTimestamped[days1.type, V](ts, v),
              Decay.fromTimestamped[days3.type, V](ts, v),
              Decay.fromTimestamped[days14.type, V](ts, v)
            )
        }

      val feat = tupled.sum

      feat.mapWithKeyTime {
        case (_, (d1, d2, d3, d4, d5, d6), ts) =>
          Decays(
            d1.atTimestamp(ts),
            d2.atTimestamp(ts),
            d3.atTimestamp(ts),
            d4.atTimestamp(ts),
            d5.atTimestamp(ts),
            d6.atTimestamp(ts)
          )
      }
    }
  }

  case class ZeroHistoryFeatures(
      ts: Timestamp,
      symbol: Symbol,
      minuteOfDay: Int,
      dayOfWeek: Int,
      log: Values[Double],
      lin: Values[Double],
      turnover_usd: Double
  )

  val zeroHistory: Event[ZeroHistoryFeatures] =
    src
      .map(candle => (candle.currency, candle))
      .postLookup(latestExchange)
      .values
      .withTime
      .map {
        case ((c, exch), ts) =>
          val v = Values.fromCandle(c)
          ZeroHistoryFeatures(
            ts,
            c.symbol,
            ts.unixMinuteOfDay,
            ts.unixDayOfWeek,
            log = v.map(d => math.log(d + 1e-6)),
            lin = v,
            turnover_usd = exch match {
              case None    => Double.NaN
              case Some(e) => e.toUSD(c.close) * c.volume
            }
          )
      }

  val decayedFeatures
      : Feature[Symbol, Decays[(Values[Moments2], Values[Moments2])]] =
    Decays.feature(
      zeroHistory
        .map { zh =>
          (zh.symbol, (Values.moments(zh.log), Values.moments(zh.lin)))
        }
    )

  case class Result(
      zeroHistory: ZeroHistoryFeatures,
      logMoments: Decays[Values[Moments2]],
      linMoments: Decays[Values[Moments2]],
      logZ: Decays[Values[Double]],
      linZ: Decays[Values[Double]],
      target15: Target,
      target30: Target,
      target15Gain: Double,
      target30Gain: Double
  )

  val labeled: LabeledEvent[Result] = {
    val ev =
      zeroHistory
        .map(zh => (zh.symbol, zh))
        .postLookup(decayedFeatures)

    val lab1530: Label[Symbol, (Target, Target)] =
      label
        .lookForward(Duration.minutes(15))
        .zip(label.lookForward(Duration.minutes(30)))

    LabeledEvent(ev, lab1530)
      .map {
        case (_, ((zh, decayPair), (t15, t30))) =>
          val logM = decayPair.map(_._1)
          val linM = decayPair.map(_._2)
          val logZ = logM.map { v =>
            v.zip(zh.log).map { case (m, d) => m.zscore(d) }
          }
          val linZ = linM.map { v =>
            v.zip(zh.lin).map { case (m, d) => m.zscore(d) }
          }
          val t15Gain = (t15.close / zh.lin.close) - 1.0
          val t30Gain = (t30.close / zh.lin.close) - 1.0

          Result(zh, logM, linM, logZ, linZ, t15, t30, t15Gain, t30Gain)
      }
  }

  // TODO: are empty bars handled correctly -- ie present with zero volume?
  // what values do we use for open/high/low/close?
  // or do we not even read them, for now?
  def candles_sql(exch_code: ExchangeCode, limit: Int) = sql"""
          SELECT
              symbol,
              candle_start_epoch_millis,
              candle_end_epoch_millis,
              open,
              high,
              low,
              close,
              volume,
              exch_code,
              currency
          FROM finnhub.stock_candles1
          WHERE exch_code = $exch_code
          ORDER BY candle_end_epoch_millis
          LIMIT $limit
         """

  val exchange_rates_sql = sql"""
          SELECT
            base_currency,
            timestamp_epoch_millis,
            one_base_in_quote
          FROM finnhub.exchange_rates
          WHERE quote_currency = 'USD'
          ORDER BY timestamp_epoch_millis
          """

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def dbSupportFactory: DBSupport.Factory =
    db.DBSupport
      .factoryFor(src, candles_sql(exch_code, sqlQueryLimit))
      .combine(db.DBSupport.factoryFor(valueInUSD, exchange_rates_sql))

  def eventArgs: Args = Args.labeledEvent[Result](labeled)

  lazy val transactor: doobie.Transactor[IO] =
    Databases
      .rdsPostgresLocalTunnel(blocker)(Async[IO], contextShift)
      .unsafeRunSync()
      .apply(Databases.pmdbProd)
}

class FinnhubDBCandle1Lambda extends aws.LambdaApp(FinnhubDBCandle1.eventArgs) {
  override def setup: IO[aws.DBS3App] =
    IO {
      new aws.DBS3App {
        def dbSupportFactory: DBSupport.Factory =
          FinnhubDBCandle1.dbSupportFactory

        lazy val transactor: doobie.Transactor[IO] =
          Databases
            .rdsPostgres(blocker)(Async[IO], contextShift)
            .unsafeRunSync()
            .apply(Databases.pmdbProd)
      }
    }
}
