package dev.posco.hiona.aws

import cats.data.NonEmptyList
import io.circe.{Encoder, Json}
import org.scalacheck.{Arbitrary, Gen, Prop}

import PuaAws.{Action, Call}
import Arbitrary.{arbitrary => arb}

object PuaAwsActionGens {

  // random length of expected size 1
  lazy val genSizeExp1: Gen[Int] =
    Gen
      .oneOf(true, false)
      .flatMap {
        case true  => Gen.const(0)
        case false => genSizeExp1.map(_ + 1)
      }

  lazy val genJson: Gen[Json] = {
    val recur = Gen.lzy(genJson)
    val kv = Gen.zip(arb[String], recur)
    Gen.frequency(
      4 -> arb[String].map(Json.fromString),
      4 -> arb[Int].map(Json.fromInt),
      4 -> arb[Long].map(Json.fromLong),
      3 -> arb[Boolean].map(Json.fromBoolean),
      3 -> arb[Double].map(Json.fromDoubleOrNull),
      2 -> Gen.const(Json.Null),
      1 -> genSizeExp1.flatMap(Gen.listOfN(_, recur)).map(Json.fromValues),
      1 -> genSizeExp1.flatMap(Gen.listOfN(_, kv)).map(Json.fromFields)
    )
  }

  lazy val genAction: Gen[Action] = {
    val genConst =
      Gen.zip(genJson, arb[Long], arb[Long], arb[Option[Long]]).map {
        case (a, b, c, d) => Action.ToConst(a, b, c, d)
      }

    val genCb =
      Gen
        .zip(
          Gen.identifier.map(LambdaFunctionName(_)),
          arb[Long],
          arb[Long],
          arb[Option[Long]]
        )
        .map { case (a, b, c, d) => Action.ToCallback(a, b, c, d) }

    lazy val genNEL: Gen[NonEmptyList[Long]] =
      for {
        len <- genSizeExp1
        list <- Gen.listOfN(len + 1, arb[Long])
      } yield NonEmptyList.fromListUnsafe(list)

    val genMakeList =
      Gen.zip(genNEL, arb[Long], arb[Option[Long]]).map {
        case (a, b, c) => Action.MakeList(a, b, c)
      }

    val genUnList =
      Gen.zip(arb[Long], genNEL, arb[Option[Long]]).map {
        case (a, b, c) => Action.UnList(a, b, c)
      }

    val genAS =
      Gen.choose(0, 1000).map(Action.AllocSlots(_))

    val genCS =
      Gen.zip(arb[Long], genJson).map {
        case (s, j) => Action.CompleteSlot(s, PuaAws.Or.First(j))
      }

    val genRS =
      arb[Long].map(Action.ReadSlot(_))

    Gen.oneOf(
      genConst,
      genCb,
      genMakeList,
      genUnList,
      Gen.const(Action.InitTables),
      Gen.const(Action.CheckTimeouts),
      genAS,
      genCS,
      genRS
    )
  }

  val genCall: Gen[Call] = {
    lazy val genExp1: Gen[Int] =
      Gen.oneOf(true, false).flatMap {
        case true  => Gen.const(0)
        case false => genExp1.map(_ + 1)
      }

    val genNot: Gen[Call.Notification] =
      Gen.zip(Gen.identifier.map(LambdaFunctionName(_)), genJson).map {
        case (n, j) => Call.Notification(n, j)
      }

    val genInv: Gen[Call.Invocation] =
      Gen.zip(genNot, Gen.choose(0L, 1000L)).map {
        case (Call.Notification(n, j), slot) => Call.Invocation(n, j, slot)
      }

    val genRep: Gen[Call.Repeated] =
      genExp1
        .flatMap(Gen.listOfN(_, Gen.oneOf(genNot, genInv)))
        .map(Call.Repeated(_))

    Gen.oneOf(genNot, genInv, genRep)
  }
}

class PuaAwsActionJsonLaws extends munit.ScalaCheckSuite {
  property("we can round-trip actions") {
    Prop.forAll(PuaAwsActionGens.genAction) { action =>
      val json = Encoder[Action].apply(action)

      assertEquals(json.as[Action], Right(action))
    }
  }

  property("we can round-trip Calls") {
    Prop.forAll(PuaAwsActionGens.genCall) { call =>
      val json = Encoder[Call].apply(call)

      assertEquals(json.as[Call], Right(call), s"json = $json")
    }
  }
}