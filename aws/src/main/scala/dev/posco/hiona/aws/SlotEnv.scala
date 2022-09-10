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

import cats.data.NonEmptyList
import cats.{Applicative, Monad}
import io.circe.Json

import cats.implicits._

abstract class SlotEnv {

  // some ID in a data-base we can use
  // to find a pointer to result data
  type SlotId

  /**
    * This tracks a value A and the set of opened
    * slots
    */
  type Slots[A]
  implicit def applicativeSlots: Applicative[Slots]

  type Eff[A]
  implicit def monadEff: Monad[Eff]

  // run the lambda as an event-like function with a given
  // location to put the result
  // this is also responsible for updating the state of the db
  // if the arg slot is not yet ready, we update the list of
  // nodes waiting on the arg
  def toFnLater(ln: LambdaFunctionName, arg: SlotId, out: SlotId): Eff[Unit]

  // We have to wait for inputs to be ready for constant
  // functions because we need to order effects correctl;
  def toConst(json: Json, arg: SlotId, out: SlotId): Eff[Unit]

  // This creates a new entry in the database for an output location
  // only one item should ever write to it
  def allocSlot(pua: Pua): Slots[SlotId]

  // this is really a special job just waits for all the inputs
  // and finally writes to an output
  def makeList(inputs: NonEmptyList[SlotId], output: SlotId): Eff[Unit]

  // opposite of makeList, wait on a slot, then unlist it
  def unList(input: SlotId, outputs: NonEmptyList[SlotId]): Eff[Unit]

  def start(p: Pua): Slots[SlotId => Eff[SlotId]] = {
    import Pua._

    p match {
      case c @ Const(toJson) =>
        // we have to wait on a slot since functions
        // are effectful
        allocSlot(c).map { res => s: SlotId => toConst(toJson, s, res).as(res) }
      case c @ Call(nm) =>
        allocSlot(c).map { out => arg: SlotId =>
          toFnLater(nm, arg, out).as(out)
        }
      case Compose(f, s) =>
        (start(f), start(s)).mapN { (fstart, sstart) => arg: SlotId =>
          fstart(arg).flatMap(sstart)
        }

      case f @ Fanout(lams) =>
        (lams.traverse(start), allocSlot(f)).mapN {
          (innerFns, res) => arg: SlotId =>
            for {
              inners <- innerFns.traverse(_(arg))
              _ <- makeList(inners, res)
            } yield res
        }
      case Identity =>
        applicativeSlots.pure { s: SlotId => monadEff.pure(s) }
      case p @ Parallel(pars) =>
        // [a1 => b1, a2 => b2, .. ]
        // to [a1, a2, a3, ...] => [b1, b2, b3, ...]
        (pars.traverse(start), pars.traverse(allocSlot), allocSlot(p)).mapN {
          (innerFns, inSlots, res) => arg: SlotId =>
            for {
              _ <- unList(arg, inSlots)
              outSlots <- innerFns.zipWith(inSlots)(_(_)).sequence
              _ <- makeList(outSlots, res)
            } yield res
        }
    }
  }

}
