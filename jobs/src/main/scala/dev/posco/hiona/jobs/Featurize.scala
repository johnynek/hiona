package dev.posco.hiona.jobs

import dev.posco.hiona._
import doobie.implicits._

/**
  * Code useful in featurization of both Tick and Candle data
  */
object Featurize {

  type Symbol = String
  type Currency = String
  type ExchangeCode = String

  // region CurrencyExchange

  /**
    * CurrencyExchange events provide time-varying information about how a base currency (eg HKD) is faring in
    * quote currency (for our purposes, USD) at timestamp_epoch_millis
    * @param base_currency the currency that is being valued
    * @param timestamp_epoch_millis unix epoch (utc) when this measurement is at
    * @param one_quote_in_base how much eg 1 HKD trades for in USD
    */
  case class CurrencyExchange(
      base_currency: Currency,
      timestamp_epoch_millis: Long,
      one_quote_in_base: Double
  ) {
    def toUSD(localAmt: Double): Double = localAmt / one_quote_in_base
  }

  val valueInUSD: Event.Source[CurrencyExchange] =
    Event.source[CurrencyExchange](
      "finnhub.exchange_rates",
      Validator.pure(ce => Timestamp(ce.timestamp_epoch_millis))
    )

  val latestExchange: Feature[Currency, Option[CurrencyExchange]] =
    valueInUSD
      .map(ce => (ce.base_currency, ce))
      .latest

  val mockExchange: Feature[Currency, Option[CurrencyExchange]] =
    Feature.fromFn {
      case "USD" => Some(CurrencyExchange("USD", 0L, 1.0))
      case _     => None
    }

  val exchange_rates_sql = sql"""
          SELECT
            base_currency,
            timestamp_epoch_millis,
            one_quote_in_base
          FROM finnhub.exchange_rates
          WHERE quote_currency = 'USD'
          ORDER BY timestamp_epoch_millis
          """
  // endregion CurrencyExchange

}
