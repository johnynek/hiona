package dev.posco.hiona.jobs

import cats.Monoid
import cats.effect.{Async, IO, Resource}
import cats.implicits._
import com.monovore.decline.{Command, Opts}
import dev.posco.hiona._
import dev.posco.hiona.db.DBSupport
import dev.posco.hiona.jobs.Featurize._
import doobie.implicits._
import doobie.util.fragment.Fragment

/**
  * Specify and evaluate a computation graph
  */
object FinnhubDBCandle extends aws.DBS3CliApp {

  // TODO: maybe gain names that are suffixed target names (...+ '.gain' - can "_" become "."?)
  // TODO: adjust lookforward amounts for labels
  // TODO: featurize volume spikes on declines, featurize price digits
  // TODO: is candle_end_epoch_millis too early for "overnight candles"?? should it be checked against next start instead?
  // TODO: are empty bars handled correctly -- ie present with zero volume?
  // what values do we use for open/high/low/close? or do we not even read them, for now?

  def candles_sql(
      candlesView: String,
      symbolsView: String,
      exchCode: ExchangeCode
  ): doobie.Query0[Candle] =
    (fr"""
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
          FROM""" ++
      Fragment.const(candlesView) ++
      fr""" AS C
      INNER JOIN (SELECT DISTINCT symbol AS _symbol FROM
      """ ++
      Fragment.const(symbolsView) ++
      fr""") AS S
          ON symbol = _symbol
          WHERE exch_code = $exchCode
          ORDER BY candle_end_epoch_millis, symbol
         """).query[Candle]

  /**
    * This describes the form of the input data. In this case, mirrors
    * the DB table from which the data comes
    */
  case class Candle(
      symbol: Symbol,
      candleStartEpochMillis: Long,
      candleEndEpochMillis: Long,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Double,
      exchCode: ExchangeCode,
      currency: Currency
  ) {
    def change: Double = close / open
    def priceRange: Double = high / low

    def toValues: Values[Double] = Values(close, change, priceRange, volume)
  }

  val src: Event.Source[Candle] = Event.source[Candle](
    "finnhub.stock_candles",
    Validator.pure(candle => Timestamp(candle.candleEndEpochMillis))
  )

  val latestCandle: Feature[Symbol, Option[Candle]] =
    src.latestBy(_.symbol)

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

    def moments(vd: Values[Double]): Values[Moments2] =
      vd.map(Moments2.value)
  }

  // endregion Values

  case class MetaFeatures(ts: Timestamp, symbol: Symbol)

  /**
    * ZeroHistoryFeatures are independent of earlier values and are hence simple to compute
    */
  case class ZeroHistoryFeatures(
      minuteOfDay: Int,
      dayOfWeek: Int,
      turnoverUsd: Double,
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
            turnoverUsd = exch match {
              case None    => Double.NaN
              case Some(e) => e.toUSD(c.close) * c.volume
            },
            lin = v,
            log = v.map(d => math.log(d + 1e-6))
          )
          (MetaFeatures(ts, c.symbol), zh)
      }

  case class Window10Values(turnoverUsd: Double, volume: Double)
  val turnoverVol10Feature: Feature[Symbol, Window10Values] =
    metaAndZeroHistory
      .map { case (m, z) => (m.symbol, (z.turnoverUsd, z.lin.volume)) }
      .windowSum(Duration.minutes(10L))
      .map { case (t, v) => Window10Values(t, v) }

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

  val turnoverVol10Label: Label[Symbol, Window10Values] =
    Label(turnoverVol10Feature).lookForward(Duration.minutes(10L))

  /**
    * Bundles together all the Targets of the computation
    */
  case class Targets(
      entryWindow: Window10Values,
      mins15: Target,
      mins30: Target,
      gainMins15: Double,
      gainMins30: Double,
      exit15Window: Window10Values,
      exit30Window: Window10Values
  )

  case class Result(
      metaFeatures: MetaFeatures,
      targets: Targets,
      zeroHistory: ZeroHistoryFeatures,
      linMoments: Decays[Values[Moments2]],
      logMoments: Decays[Values[Moments2]],
      linZ: Decays[Values[Double]],
      logZ: Decays[Values[Double]]
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

    val min15 = Duration.minutes(15)
    val min30 = Duration.minutes(30)

    val lab15: Label[Symbol, Target] = label.lookForward(min15)
    val lab30: Label[Symbol, Target] = label.lookForward(min30)

    val allLabs: Label[
      Symbol,
      ((((Window10Values, Target), Target), Window10Values), Window10Values)
    ] =
      turnoverVol10Label
        .zip(lab15)
        .zip(lab30)
        .zip(turnoverVol10Label.lookForward(min15))
        .zip(turnoverVol10Label.lookForward(min30))

    LabeledEvent(ev, allLabs)
      .map {
        case (_, (((meta, zh), decayPair), ((((w0, t15), t30), w15), w30))) =>
          val linM = decayPair.map(_._1)
          val logM = decayPair.map(_._2)
          val linZ = linM.map { v =>
            v.zip(zh.lin).map { case (m, d) => m.zscore(d) }
          }
          val logZ = logM.map { v =>
            v.zip(zh.log).map { case (m, d) => m.zscore(d) }
          }

          val gainMins15 = (t15.close / zh.lin.close) - 1.0
          val gainMins30 = (t30.close / zh.lin.close) - 1.0
          val targets = Targets(w0, t15, t30, gainMins15, gainMins30, w15, w30)

          Result(meta, targets, zh, linM, logM, linZ, logZ)
      }
  }

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def dbSupportFactory: IO[DBSupport.Factory] = {
    val candlesView = "finnhub.stock_candles_5min"
    val symbolsView = "finnhub.stock_symbols_view"
    val exchCode: ExchangeCode = "US" // "HK", "T"

    // get from env vars, which are set in payload
    val dbCmd = Command("finnhub_db_candle", "finnhub db env args") {
      (
        Opts
          .env[String]("db_candles_view", "sql view or table of candle data")
          .orElse(Opts(candlesView)),
        Opts
          .env[String](
            "db_symbols_view",
            "sql view or table of symbols to limit to"
          )
          .orElse(Opts(symbolsView)),
        Opts
          .env[String](
            "db_exchange",
            "the exchange code to limit db_sql_view to"
          )
          .orElse(Opts(exchCode))
      ).mapN((_, _, _))
    }

    IOEnv
      .readArgs(dbCmd)
      .map {
        case (candlesView, symbolsView, exchCode) =>
          db.DBSupport
            .factoryFor(src)(candles_sql(candlesView, symbolsView, exchCode))
            .combine(
              db.DBSupport.factoryFor(valueInUSD)(exchange_rates_sql.query)
            )
      }
  }

  def eventOutput: Output = Output.labeledEvent[Result](labeled)

  val transactor: Resource[IO, doobie.Transactor[IO]] =
    Resource.liftF(
      Databases
        .rdsPostgresLocalTunnel(Databases.pmdbProd, blocker)(
          Async[IO],
          contextShift
        )
    )
}

class FinnhubDBCandleLambda
    extends aws.DBLambdaApp0(
      FinnhubDBCandle.eventOutput,
      FinnhubDBCandle.dbSupportFactory,
      Databases.pmdbProdTransactor[IO]
    )
