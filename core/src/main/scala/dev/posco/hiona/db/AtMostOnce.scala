package dev.posco.hiona.db

import cats.{Applicative, MonadError}
import cats.effect.IO
import doobie._

import doobie.implicits._
import cats.implicits._

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
