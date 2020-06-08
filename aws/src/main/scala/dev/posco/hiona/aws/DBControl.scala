package dev.posco.hiona.aws

import cats.effect.IO
import doobie.ConnectionIO
import io.circe.Json

abstract class DBControl {

  def initializeTables: ConnectionIO[Unit]

  def cleanupTables: ConnectionIO[Unit]

  def run[A](c: ConnectionIO[A]): IO[A]

  def allocSlots(count: Int): ConnectionIO[List[Long]]

  def readSlot(
      slotId: Long
  ): ConnectionIO[Option[PuaAws.Or[PuaAws.Error, Json]]]

  def addWaiter(
      act: PuaAws.BlockingAction,
      function: LambdaFunctionName
  ): ConnectionIO[Unit]

  def removeWaiter(act: PuaAws.BlockingAction): ConnectionIO[Unit]

  def completeSlot(
      slotId: Long,
      result: PuaAws.Or[PuaAws.Error, Json]
  ): ConnectionIO[List[(LambdaFunctionName, Json)]]

  def resendNotifications: ConnectionIO[List[(LambdaFunctionName, Json)]]
}
