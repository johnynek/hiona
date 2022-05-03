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
