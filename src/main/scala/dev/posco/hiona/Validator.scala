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

  def parseAndShift[A](
      parseString: String,
      tz: TimeZone,
      get: A => String,
      shift: Timestamp => Timestamp
  ): Validator[A] =
    new Validator[A] {
      import java.text.SimpleDateFormat

      val fmt = new SimpleDateFormat(parseString)
      //fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
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
