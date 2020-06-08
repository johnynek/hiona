package dev.posco.hiona.jobs

import cats.effect.{Async, Blocker, ContextShift}
import dev.posco.hiona._
import doobie.Transactor
import dev.posco.hiona.aws.RDSTransactor.DatabaseName

object Databases {
  val pmdbProd = DatabaseName("pmdb_prod")

  def rdsPostgresLocalTunnel[F[_]: Async](
      dbName: DatabaseName,
      blocker: Blocker
  )(implicit
      ctx: ContextShift[F]
  ): F[doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        blocker = blocker,
        dbName = dbName,
        hostPort = Some(aws.RDSTransactor.HostPort("127.0.0.1", 54320))
      )

  def rdsPostgres[F[_]: Async](dbName: DatabaseName, blocker: Blocker)(implicit
      ctx: ContextShift[F]
  ): F[doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        dbName = dbName,
        blocker = blocker
      )

  def pmdbProdTransactor[F[_]: Async]
      : (Blocker, ContextShift[F]) => F[Transactor[F]] = { (blocker, ctx) =>
    implicit val ictx: ContextShift[F] = ctx
    rdsPostgres[F](pmdbProd, blocker)
  }
}
