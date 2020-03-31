package dev.posco.hiona

/**
 * A Validator validates the shape/constraints of a deserialized row of data
 * and it extracts a valid timestamp. If those fail, we instead return a Validator.Error
 *
 * We will eventually make it configurable how to fail, but currently any failure is a hard
 * failure. In a realtime system, we might instead just log the failure and keep going.
 */
trait Validator[A] {
  def validate(a: A): Either[Validator.Error, Timestamp]
}

object Validator {
  sealed abstract class Error(msg: String) extends Exception(msg)

  case class MissingTimestamp[A](from: A) extends Error(s"value $from has a missing timestamp")

  case class TimestampParseFailure[A](from: A, badString: String) extends Error(s"couldn't parse: $badString in $from")
}

