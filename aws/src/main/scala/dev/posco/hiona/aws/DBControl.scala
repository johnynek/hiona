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
