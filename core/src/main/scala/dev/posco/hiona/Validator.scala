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

package dev.posco.hiona

import java.util.TimeZone

/**
  * A Validator validates the shape/constraints of a deserialized row of data
  * and it extracts a valid timestamp. If those fail, we instead return a Validator.Error
  *
  * We will eventually make it configurable how to fail, but currently any failure is a hard
  * failure. In a realtime system, we might instead just log the failure and keep going.
  */
trait Validator[-A] {
  def validate(a: A): Either[Validator.Error, Timestamp]
}

object Validator {
  sealed abstract class Error(msg: String) extends Exception(msg)

  case class MissingTimestamp[A](from: A)
      extends Error(s"value $from has a missing timestamp")

  case class TimestampParseFailure[A](from: A, badString: String)
      extends Error(s"couldn't parse: $badString in $from")

  def pure[A](fn: A => Timestamp): Validator[A] =
    new Validator[A] {
      def validate(a: A) = Right(fn(a))
    }

  def fromEpochSecondsStr[A](get: A => String): Validator[A] =
    new Validator[A] {
      def validate(a: A): Either[TimestampParseFailure[Any], Timestamp] = {
        val ts = get(a)
        try Right(Timestamp(ts.toLong * 1000))
        catch {
          case _: NumberFormatException =>
            Left(Validator.TimestampParseFailure(a, ts))
        }
      }
    }

  def parseAndShift[A](
      parseString: String,
      tz: TimeZone,
      get: A => String,
      shift: Timestamp => Timestamp
  ): Validator[A] =
    new Validator[A] {
      import java.text.SimpleDateFormat

      val fmt = new SimpleDateFormat(parseString)
      // fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
      fmt.setTimeZone(tz)

      def validate(a: A): Either[Validator.Error, Timestamp] = {
        val str = get(a)
        try {
          // the bar is only available at the end of the next bar,
          // these are 30 minute bars:
          val ts = fmt.parse(str).getTime
          Right(shift(Timestamp(ts)))
        } catch {
          case (_: java.text.ParseException) =>
            Left(Validator.TimestampParseFailure(a, str))
        }
      }
    }

  def parseAndShiftUtc[A](
      parseString: String,
      get: A => String,
      shift: Timestamp => Timestamp
  ): Validator[A] =
    parseAndShift(parseString, TimeZone.getTimeZone("UTC"), get, shift)

  implicit class ValidatorOps[A](private val validator: Validator[A])
      extends AnyVal {
    def shiftLater(dur: Duration): Validator[A] =
      new Validator[A] {
        def validate(a: A) = validator.validate(a).map(_ + dur)
      }
  }
}
