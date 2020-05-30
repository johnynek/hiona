package dev.posco.hiona.jobs

import cats.effect.{Async, IO}
import cats.implicits._
import dev.posco.hiona._
import dev.posco.hiona.db.DBSupport
import doobie.implicits._

object Candle1DBJob extends aws.DBS3CliApp {

  val candleResolution: String = "1"
  val millisPerCandle: Long = 60 * 1000

  case class Candle1(
      symbol: String,
      startTimestampMillis: Long,
      endTimestampMillis: Long,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Double,
      exch_code: String,
      currency: String
  )

  case class CurrencyExchange(
      base_currency: String,
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

  val src: Event.Source[Candle1] = Event.source[Candle1](
    "finnhub.stock_candles",
    Validator.pure(candle => Timestamp(candle.endTimestampMillis))
  )

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def candleDbSupportFactory: DBSupport.Factory =
    db.DBSupport
      .factoryFor(
        src,
        sql"""
          SELECT *
          FROM finnhub.stock_candles1
          WHERE exch_code = 'HK'
          ORDER BY candle_end_epoch_millis
          LIMIT 1000
         """
      )

  def currencyExchangeDbSupportFactory: DBSupport.Factory =
    db.DBSupport
      .factoryFor(
        valueInUSD,
        sql"""
          SELECT
            base_currency,
            timestamp_epoch_millis,
            one_base_in_quote
          FROM finnhub.exchange_rates
          WHERE quote_currency = 'USD'
          ORDER BY timestamp_epoch_millis
         """
      )

  def dbSupportFactory: DBSupport.Factory =
    candleDbSupportFactory.combine(currencyExchangeDbSupportFactory)

  val symbolCount: Event[(String, (Timestamp, Long))] = {
    val feat: Feature[String, Long] = src.map(c => (c.symbol, 1L)).sum

    src.withTime
      .map { case (c, ts) => (c.symbol, ts) }
      .postLookup(feat)
  }

  def eventArgs: Args = Args.event(symbolCount)

  lazy val transactor: doobie.Transactor[IO] =
    Databases
      .rdsPostgresLocalTunnel(blocker)(Async[IO], contextShift)
      .unsafeRunSync()
      .apply(Databases.pmdbProd)
}

class Candle1DBLambdaJob extends aws.LambdaApp(Candle1DBJob.eventArgs) {
  override def setup =
    IO {
      new aws.DBS3App {
        def dbSupportFactory = Candle1DBJob.dbSupportFactory

        lazy val transactor =
          Databases
            .rdsPostgres(blocker)(Async[IO], contextShift)
            .unsafeRunSync()
            .apply(Databases.pmdbProd)
      }
    }
}
