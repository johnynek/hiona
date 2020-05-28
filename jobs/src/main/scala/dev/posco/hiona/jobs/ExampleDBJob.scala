package dev.posco.hiona.jobs

import cats.effect.IO
import dev.posco.hiona._

import cats.implicits._
import doobie.implicits._

object ExampleDBJob extends aws.DBS3CliApp {

  case class Candle(_symbol: String, timestamp: String, open: String)

  val src: Event.Source[Candle] = Event.source[Candle](
    "finnhub.stock_candles",
    Validator.fromEpochSecondsStr(_.timestamp)
  )

  /**
    * This is what binds the the Event.Source to particular
    * sql queries
    * DBSupport.factoryFor(src, "some sqlString here")
    */
  def dbSupportFactory =
    db.DBSupport
      .factoryFor(
        src,
        //sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL order by timestamp asc limit 10000"
        sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL limit 10000"
      )

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
      ExampleDBJob.dbSupportFactory,
      Databases.pmdbProdTransactor[IO]
    )
