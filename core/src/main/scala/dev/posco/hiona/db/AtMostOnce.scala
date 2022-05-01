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

package dev.posco.hiona.db

import cats.effect.IO
import cats.{Applicative, MonadError}
import doobie._

import cats.implicits._
import doobie.implicits._

object AtMostOnce {
  def createTable: ConnectionIO[Unit] =
    sql"""CREATE TABLE aws_request_lock(
      request_id VARCHAR NOT NULL UNIQUE,
      created TIMESTAMP NOT NULL DEFAULT NOW(),
      token TEXT
    )""".update.run.void

  def zeroOrOne[A](q: Query0[A]): ConnectionIO[Option[A]] =
    q.stream.take(2).compile.toList.flatMap {
      case Nil      => Applicative[ConnectionIO].pure(None)
      case a :: Nil => Applicative[ConnectionIO].pure(Some(a))
      case twoOrMore =>
        MonadError[ConnectionIO, Throwable].raiseError(
          new Exception(
            s"expected 0 or 1 tokens in aws_request_lock for a given id, found: $twoOrMore"
          )
        )
    }

  def createRequest(
      requestId: String,
      token: String
  ): ConnectionIO[Either[String, Unit]] =
    for {
      existing <- zeroOrOne(
        sql"select token from aws_request_lock where request_id = $requestId"
          .query[String]
      )
      res <- existing match {
        case None =>
          sql"insert into aws_request_lock (request_id, token) values ($requestId, $token)".update.run
            .as(Right(()))
        case Some(existingToken) =>
          Applicative[ConnectionIO].pure(Left(existingToken))
      }
    } yield res

  def assertUnique(
      requestId: String,
      token: String,
      transactor: Transactor[IO]
  ): IO[Unit] =
    (createTable.attempt.transact(transactor) *>
      createRequest(requestId, token).transact(transactor)).flatMap {
      case Right(_) => IO.unit
      case Left(existing) =>
        IO.raiseError(
          new Exception(
            s"for request_id $requestId could not create token $token, existing: $existing"
          )
        )
    }
}
