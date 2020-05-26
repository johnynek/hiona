package dev.posco.hiona.aws

import cats.data.NonEmptyList
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, ContextShift, IO, LiftIO, Resource}
import cats.Monad
import com.amazonaws.services.lambda.runtime.Context
import doobie._
import doobie.enum.TransactionIsolation.TransactionSerializable
import io.circe.{Encoder, Json}
import org.scalacheck.Prop
import scala.concurrent.duration.FiniteDuration

import cats.implicits._
import doobie.implicits._

/*
object ImpLog {
  implicit val han = doobie.util.log.LogHandler.jdkLogHandler
}

import ImpLog.han
 */
/**
  * Data structures:
  * Waiter: FnName, Json argument to send
  *
 * key: slot: Long, update: Timestamp, result: Option[Json], errNumber: Option[Int], failure: Option[String], waiters: Option[List[Waiter]]
  case class Slot(
      slotId: Long,
      update: SQLTimestamp,
      result: Option[String],
      errorCode: Option[Int],
      errorMessage: Option[String],
      waiters: List[Long]
  )
  case class Waiter(
      waiterId: Long,
      update: SQLTimestamp,
      functionName: String,
  )
  */
class H2DBControl(val transactor: Transactor[IO], sem: Semaphore[IO])
    extends DBControl {

  /*
  def setSerializable =
    // from H2 docs.....
    // HC.setTransactionIsolation(TransactionSerializable) should work
    FC.setAutoCommit(false) *>
      sql"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.void
   */

  def initializeTables: ConnectionIO[Unit] = {
    val t1 = sql"""CREATE TABLE slots(
      slotid bigint auto_increment,
      result text,
      error_number int,
      failure text,
      waiters text
    )""".update.run

    val t2 = sql"""CREATE TABLE waiters(
      waitid bigint auto_increment,
      function_name text,
      function_arg text
    )""".update.run

    (t1 >> t2).void
  }

  def run[A](cio: ConnectionIO[A]): IO[A] =
    // we shouldn't need the semaphore to keep the transactions atomic
    sem.withPermit(cio.transact(transactor))
  //cio.transact(transactor)

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
      act: PuaAws.Action,
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

  def removeWaiter(act: PuaAws.Action): ConnectionIO[Unit] =
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

object H2DBControl {
  def apply(
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, H2DBControl] = {
    val xa0 = Transactor.fromDriverManager[IO](
      classOf[org.h2.Driver].getName,
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;LOCK_MODE=1",
      //"jdbc:h2:~/test;LOCK_MODE=1",
      "sa", // user
      "", // password
      blocker
    )

    val b = for {
      _ <- FC.setAutoCommit(false)
      _ <-
        sql"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.void
      _ <- HC.setTransactionIsolation(TransactionSerializable)
    } yield ()

    val xa = Transactor.before.set(xa0, b)

    Resource.liftF(Semaphore[IO](1).map(new H2DBControl(xa, _)))
  }
}

class PuaAwsTest extends munit.ScalaCheckSuite {
  implicit val ctx = IO.contextShift(scala.concurrent.ExecutionContext.global)

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        10
      ) // a bit slow, but locally, this passes with more

  property("h2 puaaws matches what we expect") {

    def law(
        pua: Pua,
        dir0: PuaLocal.Directory,
        inputs: List[Json],
        fn: Json => IO[Json]
    ) = {
      val computation =
        Blocker[IO].flatMap(b => H2DBControl(b).map((b, _))).use {
          case (blocker, dbControl) =>
            // make the rng deterministic
            val rng = new java.util.Random(123)
            // assign a random name for the lambda
            val lambdaName: String =
              Iterator
                .continually(rng.nextInt.toString)
                .dropWhile(nm => dir0.contains(LambdaFunctionName(nm)))
                .next

            // this is yucky, but we have a circular dependency
            // that is broken before we invoke
            val workerRef: Ref[IO, Json => IO[Unit]] =
              Ref.unsafe(_ => IO.unit)

            val invokeLam: Json => IO[Unit] = { j: Json =>
              IO.suspend {
                workerRef.get.flatMap(_(j))
              }
            }

            val dir = dir0.addFn(lambdaName, invokeLam)

            val setupH2Worker: PuaAws.State =
              PuaAws.State(dbControl, dir, blocker, ctx)

            val worker = new PuaWorker {
              override def setup =
                IO.pure(setupH2Worker)
                  .as(setupH2Worker)
            }

            val puaAws = new PuaAws(dbControl, invokeLam)

            implicit val timer =
              IO.timer(scala.concurrent.ExecutionContext.global)

            val mockContext: Context =
              new Context {
                def getAwsRequestId(): String = ???
                def getClientContext()
                    : com.amazonaws.services.lambda.runtime.ClientContext = ???
                def getFunctionName(): String = ???
                def getFunctionVersion(): String = ???
                def getIdentity()
                    : com.amazonaws.services.lambda.runtime.CognitoIdentity =
                  ???
                def getInvokedFunctionArn(): String = lambdaName
                def getLogGroupName(): String = ???
                def getLogStreamName(): String = ???
                def getLogger()
                    : com.amazonaws.services.lambda.runtime.LambdaLogger = ???
                def getMemoryLimitInMB(): Int = ???
                def getRemainingTimeInMillis(): Int = ???
              }

            val setWorker = workerRef.set { j =>
              worker
                .run(
                  IO.fromEither(j.as[PuaAws.Action]),
                  setupH2Worker,
                  mockContext
                )
                .start
                .void
            /*
             the tests fail to complete with the following
             that means this state machine is not yet safe
             with races. We probably need to have a polling
             mechanism to wake up and try again if messages
             are received out of order, or we need to be
             more careful in the state transitions
             */
            }

            val dbFn =
              puaAws.toIOFnPoll[Json, Json](pua, FiniteDuration(50, "ms"))

            for {
              _ <-
                dbControl.initializeTables
                  .transact(dbControl.transactor)
                  .attempt
              _ <- setWorker
              res <- inputs.parTraverse(dbFn)
            } yield res
        }

      (computation.attempt, inputs.traverse(fn).attempt)
        .mapN {
          case (Right(a), Right(b)) =>
            assertEquals(a, b)
          case (Left(_), Left(_)) =>
            assert(true)
          case (left, right) =>
            // this will fail, but we want to see what we get
            left match {
              case Left(err) => err.printStackTrace
              case Right(_)  => ()
            }
            assertEquals(left, right)
        }
        .unsafeRunSync()
    }

    val dir = PuaLocal.emptyDirectory
      .addFn("a", { j: Long => IO.pure(j) })
      .addFn("b", { j: Long => IO.pure(j + 1) })
      .addFn("c", { j: Json => IO.pure(j) })
      .addFn("d", { j: Json => IO.pure(j) })

    val regression0 =
      Pua.Compose(
        Pua.Fanout(NonEmptyList.of(Pua.call("a"), Pua.call("b"))),
        Pua.Compose(Pua.call("c"), Pua.call("d"))
      )

    law(
      regression0,
      dir,
      List(Json.fromLong(1L)),
      { j: Json =>
        val j1 = Json.fromLong(
          j.as[Long].getOrElse(sys.error(s"expected long in $j")) + 1L
        )
        IO.pure(Json.fromValues(List(j, j1)))
      }
    )

    Prop.forAllNoShrink(PuaGens.genDirectory) {
      case (pua, dir0, inputs, fn) => law(pua, dir0, inputs, fn)
    }
  }
}
