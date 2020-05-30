package dev.posco.hiona.jobs

import cats.Monoid
import cats.effect.Async
import cats.effect.IO
import dev.posco.hiona._
import dev.posco.hiona.db.DBSupport
import dev.posco.hiona.jobs.Featurize._
import doobie.implicits._

/**
  * Specify and evaluate a computation graph
  */
object FinnhubDBCandle1 extends aws.DBS3CliApp {

  // TODO: get from env vars, which are set in payload
  val exch_code: ExchangeCode = "HK"
  val sqlQueryLimit: Int = 1000

  // TODO: adjust lookforward amounts for labels
  // TODO: featurize volume spikes on declines, featurize price digits
  // TODO: is candle_end_epoch_millis too early for "overnight candles"?? should it be checked against next start instead?
  // TODO: are empty bars handled correctly -- ie present with zero volume?
  // what values do we use for open/high/low/close?
  // or do we not even read them, for now?

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

    def toValues: Values[Double] = Values(close, change, priceRange, volume)
  }

  val src: Event.Source[Candle] = Event.source[Candle](
    "finnhub.stock_candles",
    Validator.pure(candle => Timestamp(candle.candle_end_epoch_millis))
  )

  val latestCandle: Feature[Symbol, Option[Candle]] =
    src
      .map(candle => (candle.symbol, candle))
      .latest

  // region Values

  /**
    * Our featurization consists of "zero history" features, some labels (as in
    * labels for supervised learning), and "data cubed" features. The values
    * below are what is subjected to the "data cubing" -- for each of these,
    * various transforms will be computed
    *
    * TODO: rename Values -> CandleDecayable?
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

  // endregion Values

  case class MetaFeatures(ts: Timestamp, symbol: Symbol)

  /**
    * ZeroHistoryFeatures are independent of earlier values and are hence simple to compute
    */
  case class ZeroHistoryFeatures(
      minuteOfDay: Int,
      dayOfWeek: Int,
      turnover_usd: Double,
      lin: Values[Double],
      log: Values[Double]
  )

  val metaAndZeroHistory: Event[(MetaFeatures, ZeroHistoryFeatures)] =
    src
      .map(candle => (candle.currency, candle))
      .postLookup(latestExchange)
      .values
      .withTime
      .map {
        case ((c, exch), ts) =>
          val v = c.toValues
          val zh = ZeroHistoryFeatures(
            ts.unixMinuteOfDay,
            ts.unixDayOfWeek,
            turnover_usd = exch match {
              case None    => Double.NaN
              case Some(e) => e.toUSD(c.close) * c.volume
            },
            lin = v,
            log = v.map(d => math.log(d + 1e-6))
          )
          (MetaFeatures(ts, c.symbol), zh)
      }

  val decayedFeatures
      : Feature[Symbol, Decays[(Values[Moments2], Values[Moments2])]] =
    Decays.feature(
      metaAndZeroHistory
        .map {
          case (meta, zh) =>
            (meta.symbol, (Values.moments(zh.lin), Values.moments(zh.log)))
        }
    )

  /**
    * A Target has the values that will be predicted
    */
  case class Target(close: Double, volume: Double)

  val label: Label[Symbol, Target] = Label(latestCandle.map {
    case None    => Target(Double.NaN, 0.0)
    case Some(c) => Target(c.close, c.volume)
  })

  /**
    * Bundles together all the Targets of the computation
    */
  case class Targets(
      target15: Target,
      target30: Target,
      target15Gain: Double,
      target30Gain: Double
  )

  case class Result(
      metaFeatures: MetaFeatures,
      zeroHistory: ZeroHistoryFeatures,
      linMoments: Decays[Values[Moments2]],
      logMoments: Decays[Values[Moments2]],
      linZ: Decays[Values[Double]],
      logZ: Decays[Values[Double]],
      targets: Targets
  )

  /**
    * the stream of Result objects that will be output
    */
  val labeled: LabeledEvent[Result] = {
    val ev: Event[
      (
          Symbol,
          (
              (MetaFeatures, ZeroHistoryFeatures),
              Decays[(Values[Moments2], Values[Moments2])]
          )
      )
    ] =
      metaAndZeroHistory
        .map { case (meta, zh) => (meta.symbol, (meta, zh)) }
        .postLookup(decayedFeatures)

    val lab15: Label[Symbol, Target] = label.lookForward(Duration.minutes(15))
    val lab30: Label[Symbol, Target] = label.lookForward(Duration.minutes(30))

    val lab15_30: Label[Symbol, (Target, Target)] = lab15.zip(lab30)

    LabeledEvent(ev, lab15_30)
      .map {
        case (_, (((meta, zh), decayPair), (t15, t30))) =>
          val linM = decayPair.map(_._1)
          val logM = decayPair.map(_._2)
          val linZ = linM.map { v =>
            v.zip(zh.lin).map { case (m, d) => m.zscore(d) }
          }
          val logZ = logM.map { v =>
            v.zip(zh.log).map { case (m, d) => m.zscore(d) }
          }

          val t15Gain = (t15.close / zh.lin.close) - 1.0
          val t30Gain = (t30.close / zh.lin.close) - 1.0
          val targets = Targets(t15, t30, t15Gain, t30Gain)

          Result(meta, zh, linM, logM, linZ, logZ, targets)
      }
  }

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
          FROM finnhub.stock_candles_view
          WHERE exch_code = $exch_code
          ORDER BY candle_end_epoch_millis
          LIMIT $limit
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
