package dev.posco.hiona.aws

import cats.Monad
import cats.effect.{IO, LiftIO}
import doobie._
import io.circe.{Encoder, Json}

import cats.implicits._
import doobie.implicits._

class PostgresDBControl(transactor: Transactor[IO]) extends DBControl {

  def initializeTables: ConnectionIO[Unit] = {
    val t1 = sql"""CREATE TABLE slots(
      slotid bigint auto_increment PRIMARY KEY,
      created timestamp NOT NULL DEFAULT NOW(),
      result text,
      error_number int,
      failure text,
      waiters text
    )""".update.run

    val t2 = sql"""CREATE TABLE waiters(
      waitid bigint auto_increment PRIMARY KEY,
      created timestamp NOT NULL DEFAULT NOW(),
      function_name text,
      function_arg text
    )""".update.run

    (t1 >> t2).void
  }

  def cleanupTables: ConnectionIO[Unit] =
    for {
      _ <- sql"DROP TABLE slots".update.run
      _ <- sql"DROP TABLE waiters".update.run
    } yield ()

  def run[A](cio: ConnectionIO[A]): IO[A] =
    cio.transact(transactor)

  def allocSlot: ConnectionIO[Long] = {
    val emptyJson = "[]"
    sql"""insert into slots (waiters) values ($emptyJson)""".update
      .withUniqueGeneratedKeys[Long]("slotid")
  }

  def stringToJson(str: String): ConnectionIO[Json] =
    LiftIO[ConnectionIO]
      .liftIO(IO.fromEither(io.circe.parser.parse(str)))

  def readSlot(
      slotId: Long
  ): ConnectionIO[Option[Either[PuaAws.Error, Json]]] = {
    import shapeless._

    def toRes(
        os: Option[String],
        oerr: Option[Int],
        omsg: Option[String]
    ): ConnectionIO[Option[Either[PuaAws.Error, Json]]] =
      os match {
        case Some(jsonString) =>
          stringToJson(jsonString)
            .map(j => Some(Right(j)))
        case None =>
          oerr match {
            case Some(num) =>
              Monad[ConnectionIO].pure(
                Some(Left(PuaAws.Error(num, omsg.getOrElse(""))))
              )
            case None => Monad[ConnectionIO].pure(None)
          }
      }

    for {
      res :: errNum :: failMsg :: HNil <-
        sql"""select result, error_number, failure from slots where slotid = $slotId"""
          .query[Option[String] :: Option[Int] :: Option[String] :: HNil]
          .unique
      optEither <- toRes(res, errNum, failMsg)
      //_ = println(s"readSlot($slotId) == $optEither")
    } yield optEither
  }

  def getWaiters(slotId: Long): ConnectionIO[List[Long]] =
    for {
      ws <-
        sql"""select waiters from slots where slotid = $slotId"""
          .query[String]
          .unique
      json <- stringToJson(ws)
      ids <- LiftIO[ConnectionIO].liftIO(IO.fromEither(json.as[List[Long]]))
    } yield ids

  def addWait(slotId: Long, waitId: Long): ConnectionIO[Unit] =
    for {
      ids <- getWaiters(slotId)
      waits = waitId :: ids
      //_ = println(s"waiters on $slotId: $waits")
      _ <- setWaiters(slotId, waits)
    } yield ()

  def setWaiters(slotId: Long, waiters: List[Long]): ConnectionIO[Unit] = {
    val newIds = Encoder[List[Long]].apply(waiters).noSpaces
    sql"""update slots set waiters = $newIds where slotid = $slotId""".update.run.void
  }

  def rmWait(slotId: Long, waitId: Long): ConnectionIO[Unit] = {
    val slot = getWaiters(slotId)
      .flatMap { current =>
        val pending = current.filterNot(_ == waitId)
        setWaiters(slotId, pending)
      }
    val wait = sql"delete from waiters where waitid = $waitId".update.run

    (slot *> wait).void
  }

  def addWaiter(
      act: PuaAws.BlockingAction,
      function: LambdaFunctionName
  ): ConnectionIO[Unit] =
    act.waitId match {
      case Some(_) =>
        //println(s"$act already in the table")
        // already in the table
        Monad[ConnectionIO].unit
      case None =>
        //println(s"$act needs to be added in the table")
        val empty = ""
        val name = function.asString
        for {
          waitId <-
            sql"""insert into waiters (function_name, function_arg) values ($name, $empty)""".update
              .withUniqueGeneratedKeys[Long]("waitid")
          actWithId = act.withWaitId(waitId)
          //_ = println(s"now with id: $actWithId")
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
      json <- stringToJson(jsonStr)
    } yield (LambdaFunctionName(nm), json)

  def completeSlot(
      slotId: Long,
      result: Either[PuaAws.Error, Json],
      invoker: LambdaFunctionName => Json => IO[Json]
  ): ConnectionIO[IO[Unit]] = {
    //println(s"completeSlot($slotId, $result)")
    val updateSlot: ConnectionIO[Unit] =
      result match {
        case Right(json) =>
          sql"update slots set result = ${json.noSpaces} where slotid = $slotId".update.run.void
        case Left(PuaAws.Error(code, msg)) =>
          sql"update slots set error_number = $code, failure = $msg where slotid = $slotId".update.run.void
      }

    for {
      // todo: this is a race, the transaction hasn't happened yet, but the message is sent now
      // if the message is lost, it is never retried
      _ <- updateSlot
      waits <- getWaiters(slotId)
      calls <- waits.traverse(getArg)
      //_ = println(s"slot: $slotId notify: $waits")
      notify = calls.traverse_ {
        case (fn, arg) => invoker(fn)(arg)
      }
    } yield notify
  }
}
