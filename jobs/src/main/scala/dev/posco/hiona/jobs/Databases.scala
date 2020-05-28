package dev.posco.hiona.jobs

import cats.effect.{Async, Blocker, ContextShift}
import dev.posco.hiona._
import doobie.Transactor

import cats.implicits._

object Databases {
  val pmdbProd = aws.RDSTransactor.DatabaseName("pmdb_prod")

  def rdsPostgresLocalTunnel[F[_]: Async](blocker: Blocker)(implicit
      ctx: ContextShift[F]
  ): F[aws.RDSTransactor.DatabaseName => doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        blocker = blocker,
        hostPort = Some(aws.RDSTransactor.HostPort("127.0.0.1", 54320))
      )

  def rdsPostgres[F[_]: Async](blocker: Blocker)(implicit
      ctx: ContextShift[F]
  ): F[aws.RDSTransactor.DatabaseName => doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        blocker = blocker
      )

  def pmdbProdTransactor[F[_]: Async]
      : (Blocker, ContextShift[F]) => F[Transactor[F]] = { (blocker, ctx) =>
    implicit val ictx: ContextShift[F] = ctx
    rdsPostgres[F](blocker).map(_(pmdbProd))
  }
}
