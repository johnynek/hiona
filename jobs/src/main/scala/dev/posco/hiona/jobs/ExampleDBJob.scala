package dev.posco.hiona.jobs

import cats.effect.IO
import dev.posco.hiona._
import cats.implicits._
import dev.posco.hiona.db.DBSupport
import doobie.implicits._

object ExampleDBJob extends aws.DBS3CliApp {

  case class Candle(_symbol: String, timestamp: String, open: String)

  val src: Event.Source[Candle] = Event.source[Candle](
    "finnhub.stock_candles",
    Validator.fromEpochSecondsStr(_.timestamp)
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

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def candleDbSupportFactory =
    db.DBSupport
      .factoryFor(
        src,
        //sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL order by timestamp asc limit 10000"
        sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL limit 10000"
      )

  def currencyExchangeDbSupportFactory =
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

  override def dbSupportFactory: DBSupport.Factory =
    candleDbSupportFactory.combine(currencyExchangeDbSupportFactory)

  val symbolCount: Event[(String, (Timestamp, Long))] = {
    val feat: Feature[String, Long] = src.map(c => (c._symbol, 1L)).sum

    src.withTime
      .map { case (c, ts) => (c._symbol, ts) }
      .postLookup(feat)
  }

  def eventArgs: Args = Args.event(symbolCount)

  lazy val transactor = Databases
    .pmdbProdTransactor[IO]
    .apply(blocker, contextShift)
    .unsafeRunSync()
}

class ExampleDBLambdaJob
    extends aws.DBLambdaApp(
      ExampleDBJob.eventArgs,
      ExampleDBJob.candleDbSupportFactory,
      Databases.pmdbProdTransactor[IO]
    )
