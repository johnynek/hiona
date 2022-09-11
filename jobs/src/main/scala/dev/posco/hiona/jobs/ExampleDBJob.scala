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

import cats.effect.{IO, Resource}
import dev.posco.hiona._
import dev.posco.hiona.db.DBSupport
import dev.posco.hiona.jobs.Featurize._

import cats.implicits._
import doobie.implicits._

object ExampleDBJob extends aws.DBS3CliApp {

  case class Candle(_symbol: String, timestamp: String, open: String)

  val src: Event.Source[Candle] = Event.csvSource[Candle](
    "finnhub.stock_candles",
    Validator.fromEpochSecondsStr(_.timestamp)
  )

  val valueInUSD: Event.Source[CurrencyExchange] =
    Event.csvSource[CurrencyExchange](
      "finnhub.exchange_rates",
      Validator.pure(ce => Timestamp(ce.timestamp_epoch_millis))
    )

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def candleDbSupportFactory: DBSupport.Factory =
    db.DBSupport
      .factoryFor(src)(
        // sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL order by timestamp asc limit 10000"
        sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL limit 10000".query
      )

  def currencyExchangeDbSupportFactory: DBSupport.Factory =
    db.DBSupport
      .factoryFor(valueInUSD)(
        sql"""
          SELECT
            base_currency,
            timestamp_epoch_millis,
            one_quote_in_base
          FROM finnhub.exchange_rates
          WHERE quote_currency = 'USD'
          ORDER BY timestamp_epoch_millis
         """.query
      )

  override def dbSupportFactory: IO[DBSupport.Factory] =
    IO.pure(candleDbSupportFactory.combine(currencyExchangeDbSupportFactory))

  val symbolCount: Event[(String, (Timestamp, Long))] = {
    val feat: Feature[String, Long] = src.map(c => (c._symbol, 1L)).sum

    src.withTime
      .map { case (c, ts) => (c._symbol, ts) }
      .postLookup(feat)
  }

  def eventOutput: Output = Output.event(symbolCount)

  val transactor = Resource.eval(
    Databases
      .pmdbProdTransactor[IO]
      .apply()
  )
}

class ExampleDBLambdaJob
    extends aws.DBLambdaApp0(
      ExampleDBJob.eventOutput,
      IO.pure(ExampleDBJob.candleDbSupportFactory),
      Databases.pmdbProdTransactor[IO]
    )
