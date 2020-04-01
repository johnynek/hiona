package dev.posco.hiona

import shapeless._
import org.scalacheck.{Arbitrary, Gen}

object ShapelessGen {
  implicit val hnilGen: Arbitrary[HNil] =
    Arbitrary(Gen.const(HNil))

  implicit def hconsGen[A, B <: HList](implicit arbA: Arbitrary[A], arbB: => Arbitrary[B]): Arbitrary[A :: B] =
    Arbitrary(
      for {
        a <- arbA.arbitrary
        b <- arbB.arbitrary
      } yield a :: b)

  implicit def ccons1Gen[A](implicit arbA: Arbitrary[A]): Arbitrary[A :+: CNil] =
    Arbitrary(arbA.arbitrary.map(Inl(_)))

  implicit def cconsGen[A, B <: Coproduct](implicit arbA: Arbitrary[A], arbB: => Arbitrary[B]): Arbitrary[A :+: B] =
    // TODO this is biased to the front for long coproducts
    Arbitrary(Gen.oneOf(true, false)
      .flatMap {
        case true => arbA.arbitrary.map(Inl(_))
        case false => arbB.arbitrary.map(Inr(_))
      })

  implicit def genericGen[A, B](implicit gen: Generic.Aux[A, B], genB: => Arbitrary[B]): Arbitrary[A] =
    Arbitrary(Gen.lzy(genB.arbitrary).map(gen.from(_)))
}
