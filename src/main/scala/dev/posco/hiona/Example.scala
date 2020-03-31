package dev.posco.hiona

object Example {
  // rows like
  // symbol,whatToShow,rth_only,bar_start_utc,open,high,low,close,volume
  // 1:HK,TRADES,False,2015-07-02 01:30:00 UTC,113.2,113.9,112.9,113.0,1039500
  case class StockData(
    symbol: String,
    whatToShow: Row.Dummy,
    rthOnly: Boolean,
    barStartUtc: String,
    open: BigDecimal,
    high: BigDecimal,
    low: BigDecimal,
    close: BigDecimal,
    volume: Long)

  object StockData {
    implicit val rowStockData: Row[StockData] =
      Row.genericRow
  }

  val dateValidator: Validator[StockData] =
    new Validator[StockData] {
      import java.text.SimpleDateFormat

      val fmt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss 'UTC'")
      fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

      def validate(sd: StockData): Either[Validator.Error, Timestamp] =
        try {
          Right(Timestamp(fmt.parse(sd.barStartUtc).getTime))
        }
        catch {
          case (_: java.text.ParseException) =>
            Left(Validator.TimestampParseFailure(sd, sd.barStartUtc))
        }
    }


  case class Highest(timestamp: Timestamp, symbol: String, highest: BigDecimal)
  object Highest {
    implicit val rowHighest: Row[Highest] = Row.genericRow
  }

  val hkStockData: Event[StockData] = Event.source("hk-stocks", dateValidator)

  val highPrice: Feature[String, Option[BigDecimal]] =
    hkStockData
      .map { sd =>
        (sd.symbol, sd.high)
      }
      .max

  def highestPriceSoFar(sd: Event[StockData]): Event[(String, BigDecimal)] =
    sd
      .withTime
      .map { case (sd, ts) => (sd.symbol, (ts, sd)) }
      .lookupAfter(highPrice)
      .map { case (symbol, ((ts, sd), high)) =>
        val v = high.getOrElse(sd.high)
        (symbol, v)
      }

  val highBarPrice: Feature[String, Option[BigDecimal]] =
    hkStockData.map { case sd => (sd.symbol, sd.high) }.latest(Duration.Infinite)

  val priceIn10Min: Label[String, Option[BigDecimal]] =
    Label(highBarPrice).lookForward(Duration.min(10))

  val predict10Min =
    LabeledEvent(highestPriceSoFar(hkStockData), priceIn10Min)
}

object ExampleApp extends LabeledApp(Example.predict10Min)
