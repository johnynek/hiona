package dev.posco.hiona.jobs

import dev.posco.hiona._

import FinnhubDBCandle.{Candle, Result, ResultModule}

object FRDCandle1 {

  def outputToCandle(o: FirstRateData.Output): Candle =
    Candle(
      symbol = o.symbol,
      candleStartEpochMillis = o.candleStartInclusiveMs.epochMillis,
      candleEndEpochMillis = o.candleEndExclusiveMs.epochMillis,
      open = o.open,
      high = o.high,
      low = o.low,
      close = o.close,
      volume = o.volume.toDouble,
      exchCode = "US",
      currency = "USD"
    )

  lazy val resultModule: ResultModule =
    new ResultModule(
      FirstRateData.Output.src.map(outputToCandle),
      Featurize.mockExchange
    )

  def eventOutput: Output = Output.labeledEvent[Result](resultModule.labeled)
}
