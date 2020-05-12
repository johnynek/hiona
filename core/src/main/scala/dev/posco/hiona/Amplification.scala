package dev.posco.hiona

/**
  * for each single event that comes out of a Source
  * how many final events come out of the Event
  */
sealed abstract class Amplification {
  def apply(b: BigInt): Boolean
  final def apply(b: Int): Boolean = apply(BigInt(b))
  def +(that: Amplification): Amplification
  def *(that: Amplification): Amplification

  def atMostOne: Boolean
}

object Amplification {

  final case class Finite(low: BigInt, high: BigInt) extends Amplification {
    require(low >= 0)
    require(high >= 0)

    def apply(b: BigInt) = (low <= b) && (b <= high)

    // when ev0 ++ ev1 this is the resulting amplification
    def +(that: Amplification) =
      that match {
        case Finite(tl, th) =>
          Finite(low.min(tl), high + th)
        case Unbounded(tl) =>
          Unbounded(low.min(tl))
      }

    // when you put two amplications in series this is the result
    def *(that: Amplification) =
      that match {
        case Zero => Zero
        case Finite(tl, th) =>
          Finite(low * tl, high * th)
        case Unbounded(tl) =>
          if (this == Zero) Zero
          else Unbounded(low * tl)
      }

    def atMostOne: Boolean = high <= BigInt(1)
  }
  final case class Unbounded(low: BigInt) extends Amplification {
    require(low >= 0)

    def apply(b: BigInt) = low <= b
    // when ev0 ++ ev1 this is the resulting amplification
    def +(that: Amplification) =
      that match {
        case Finite(tl, _) =>
          Unbounded(low.min(tl))
        case Unbounded(tl) =>
          Unbounded(low.min(tl))
      }

    // when you put two amplications in series this is the result
    def *(that: Amplification) =
      that match {
        case Zero => Zero
        case Finite(tl, _) =>
          Unbounded(low * tl)
        case Unbounded(tl) =>
          Unbounded(low * tl)
      }

    def atMostOne = false
  }

  val Zero: Amplification = Finite(BigInt(0), BigInt(0))
  val One: Amplification = Finite(BigInt(1), BigInt(1))

  val ZeroOrOne: Amplification = Finite(BigInt(0), BigInt(1))
  val ZeroOrMore: Amplification = Unbounded(BigInt(0))
}
