package dev.posco.hiona.jobs

import cats.effect.{Async, Blocker, ContextShift, IO}

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
        sql"select _symbol, timestamp, open from finnhub.stock_candles where timestamp is not NULL order by timestamp asc"
      )

  val symbolCount: Event[(String, (Timestamp, Long))] = {
    val feat: Feature[String, Long] = src.map(c => (c._symbol, 1L)).sum

    src.withTime
      .map { case (c, ts) => (c._symbol, ts) }
      .postLookup(feat)
  }

  def eventArgs: Args = Args.event(symbolCount)

  lazy val transactor =
    Databases
      .rdsPostgresLocalTunner(blocker)(Async[IO], contextShift)
      .unsafeRunSync()
      .apply(Databases.pmdbProd)
}

object Databases {
  val pmdbProd = aws.RDSTransactor.DatabaseName("pmdb_prod")

  def rdsPostgresLocalTunner[F[_]: Async](blocker: Blocker)(
      implicit ctx: ContextShift[F]
  ): F[aws.RDSTransactor.DatabaseName => doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        blocker = blocker,
        hostPort = Some(aws.RDSTransactor.HostPort("127.0.0.1", 54320))
      )

  def rdsPostgres[F[_]: Async](blocker: Blocker)(
      implicit ctx: ContextShift[F]
  ): F[aws.RDSTransactor.DatabaseName => doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        blocker = blocker
      )
}

class ExampleDBLambdaJob extends aws.LambdaApp(ExampleDBJob.eventArgs) {
  protected override def buildS3App() = new aws.DBS3App {
    def dbSupportFactory = ExampleDBJob.dbSupportFactory

    lazy val transactor =
      Databases
        .rdsPostgres(blocker)(Async[IO], contextShift)
        .unsafeRunSync()
        .apply(Databases.pmdbProd)
  }
}
