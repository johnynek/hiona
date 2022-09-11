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

package dev.posco.hiona.aws

import cats.effect.{IO, Resource}
import cats.{Monad, MonadError}
import doobie._
import io.circe.{Decoder, Encoder, Json}

import cats.implicits._
import doobie.implicits._

import PuaAws.{Error => Err, Or}

class PostgresDBControl(transactor: Transactor[IO]) extends DBControl {

  def initializeTables: ConnectionIO[Unit] = {
    val t1 = sql"""CREATE TABLE slots(
      slotid SERIAL PRIMARY KEY,
      created timestamp NOT NULL DEFAULT NOW(),
      result text,
      error_number int,
      failure text,
      completed timestamp
    )""".update.run

    val t2 = sql"""CREATE TABLE waiters(
      waitid SERIAL PRIMARY KEY,
      created timestamp NOT NULL DEFAULT NOW(),
      function_name text NOT NULL,
      function_arg text NOT NULL
    )""".update.run

    val t3 = sql"""CREATE TABLE waits(
      waitid bigint NOT NULL,
      slotid bigint NOT NULL,
      created timestamp NOT NULL DEFAULT NOW()
    )""".update.run

    (t1 >> t2 >> t3).void
  }

  def cleanupTables: ConnectionIO[Unit] =
    for {
      _ <- sql"DROP TABLE slots".update.run
      _ <- sql"DROP TABLE waiters".update.run
      _ <- sql"DROP TABLE waits".update.run
    } yield ()

  def run[A](cio: ConnectionIO[A]): IO[A] =
    cio.transact(transactor)

  def allocSlots(count: Int): ConnectionIO[List[Long]] = {
    val single =
      sql"""insert into slots (result, error_number, failure) values (NULL, NULL, NULL)""".update
        .withUniqueGeneratedKeys[Long]("slotid")

    (0 until count).toList.traverse(_ => single)
  }

  def stringTo[A: Decoder](str: String): ConnectionIO[A] =
    MonadError[ConnectionIO, Throwable]
      .fromEither(io.circe.parser.parse(str).flatMap(_.as[A]))

  private def toRes(
      os: Option[String],
      oerr: Option[Int],
      omsg: Option[String]
  ): ConnectionIO[Option[Or[Err, Json]]] =
    os match {
      case Some(jsonString) =>
        stringTo[Json](jsonString)
          .map(j => Some(Or.First(j)))
      case None =>
        oerr match {
          case Some(num) =>
            Monad[ConnectionIO].pure(
              Some(Or.Second(PuaAws.Error(num, omsg.getOrElse(""))))
            )
          case None => Monad[ConnectionIO].pure(None)
        }
    }

  def readSlot(
      slotId: Long
  ): ConnectionIO[Option[Or[Err, Json]]] = {
    import shapeless._

    for {
      res :: errNum :: failMsg :: HNil <-
        sql"""select result, error_number, failure from slots where slotid = $slotId"""
          .query[Option[String] :: Option[Int] :: Option[String] :: HNil]
          .unique
      optEither <- toRes(res, errNum, failMsg)
    } yield optEither
  }

  def getWaiters(slotId: Long): ConnectionIO[List[Long]] =
    sql"""select waitid from waits where slotid = $slotId"""
      .query[Long]
      .to[List]

  private def addWait(slotId: Long, waitId: Long): ConnectionIO[Unit] =
    sql"""insert into waits (waitid, slotid) values ($waitId, $slotId)""".update.run.void

  private def rmWait(slotId: Long, waitId: Long): ConnectionIO[Unit] =
    sql"delete from waits where waitid = $waitId AND slotid = $slotId".update.run.void

  def addWaiter(
      act: PuaAws.BlockingAction,
      function: LambdaFunctionName
  ): ConnectionIO[Unit] =
    act.waitId match {
      case Some(_) =>
        // println(s"$act already in the table")
        // already in the table
        Monad[ConnectionIO].unit
      case None =>
        // println(s"$act needs to be added in the table")
        val empty = ""
        val name = function.asString
        for {
          waitId <-
            sql"""insert into waiters (function_name, function_arg) values ($name, $empty)""".update
              .withUniqueGeneratedKeys[Long]("waitid")
          actWithId = act.withWaitId(waitId)
          // _ = println(s"now with id: $actWithId")
          json = Encoder[PuaAws.Action].apply(actWithId).noSpaces
          _ <-
            sql"""update waiters set function_arg = $json where waitid = $waitId""".update.run
          _ <- act.argSlots.traverse_(addWait(_, waitId))
        } yield ()
    }

  def removeWaiter(act: PuaAws.BlockingAction): ConnectionIO[Unit] =
    act.waitId match {
      case None => Monad[ConnectionIO].unit
      case Some(waitId) =>
        act.argSlots.traverse_(rmWait(_, waitId = waitId))
    }

  def getArg(waitId: Long): ConnectionIO[(LambdaFunctionName, Json)] =
    for {
      (nm, jsonStr) <-
        sql"select function_name, function_arg from waiters where waitid = $waitId"
          .query[(String, String)]
          .unique
      json <- stringTo[Json](jsonStr)
    } yield (LambdaFunctionName(nm), json)

  def completeSlot(
      slotId: Long,
      result: Or[Err, Json]
  ): ConnectionIO[List[(LambdaFunctionName, Json)]] = {
    // println(s"completeSlot($slotId, $result)")
    val updateSlot: ConnectionIO[Unit] =
      result match {
        case Or.First(json) =>
          sql"update slots set result = ${json.noSpaces}, completed = NOW() where slotid = $slotId".update.run.void
        case Or.Second(Err(code, msg)) =>
          sql"update slots set error_number = $code, failure = $msg, completed = NOW() where slotid = $slotId".update.run.void
      }

    for {
      _ <- updateSlot
      waits <- getWaiters(slotId)
      calls <- waits.traverse(getArg)
    } yield calls
  }

  val resendNotifications: ConnectionIO[List[(LambdaFunctionName, Json)]] = {
    val sends = sql"""
      SELECT waitid
      FROM slots INNER JOIN waits ON slots.slotid = waits.slotid
      WHERE completed IS NOT NULL"""
      .query[Long]
      .to[List]

    for {
      waits <- sends
      fnJson <- waits.traverse(getArg)
    } yield fnJson
  }
}

object PostgresDBControl {
  def apply(
      db: String,
      uname: String,
      password: String
  ): Resource[IO, PostgresDBControl] = {

    import doobie.hikari._

    val setSer = for {
      _ <- FC.setAutoCommit(false)
      _ <- HC.setTransactionIsolation(
        doobie.enumerated.TransactionIsolation.TransactionSerializable
      )
    } yield ()

    for {
      ce <- doobie.ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
      xtor <- HikariTransactor.newHikariTransactor[IO](
        classOf[org.postgresql.Driver].getName, // driver classname
        s"jdbc:postgresql://127.0.0.1/$db", // connect URL (driver-specific)
        uname,
        password,
        ce
      )
    } yield new PostgresDBControl(Transactor.before.set(xtor, setSer))
  }
}
