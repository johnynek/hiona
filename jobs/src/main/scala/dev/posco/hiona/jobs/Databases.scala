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

import cats.effect.Async
import dev.posco.hiona._
import dev.posco.hiona.aws.RDSTransactor.DatabaseName
import doobie.Transactor

object Databases {
  val pmdbProd = DatabaseName("pmdb_prod")

  def rdsPostgresLocalTunnel[F[_]: Async](
      dbName: DatabaseName
  ): F[doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        dbName = dbName,
        hostPort = Some(aws.RDSTransactor.HostPort("127.0.0.1", 54320))
      )

  def rdsPostgres[F[_]: Async](dbName: DatabaseName): F[doobie.Transactor[F]] =
    aws.RDSTransactor
      .build[F](
        region = "us-west-2",
        secretName =
          "rds-db-credentials/cluster-7HHDRW3DZNKNJWJQLJ5QRARUMY/postgres",
        dbName = dbName
      )

  def pmdbProdTransactor[F[_]: Async]: () => F[Transactor[F]] = { () =>
    rdsPostgres[F](pmdbProd)
  }
}
