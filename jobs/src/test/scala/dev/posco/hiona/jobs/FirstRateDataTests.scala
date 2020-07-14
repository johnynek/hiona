package dev.posco.hiona.jobs

import org.scalacheck.{Gen, Prop}
import scala.util.Success

import dev.posco.hiona.{Timestamp}

class FirstRateDataTests extends munit.ScalaCheckSuite {
  val genTs: Gen[Timestamp] =
    Gen.choose(0L, 2000000000L).map(Timestamp(_))

  property("dateParser works") {

    Prop.forAll(genTs) { ts =>
      val toStr = Timestamp.format(FirstRateData.format)
      val str = toStr(ts)

      val toNearestSec = Timestamp((ts.epochMillis / 1000L) * 1000L)
      assertEquals(FirstRateData.dateParser(str), Success(toNearestSec))
    }
  }

  test("we can parse symbol names") {
    val examples: List[(String, (String, Int))] =
      List(
        ("ticker_D_F/DBX_2000_2009.txt", ("DBX", 2000)),
        ("ticker_D_F/DAL_2010_2019.txt", ("DAL", 2010)),
        ("ticker_D_F/DB_2000_2009.txt", ("DB", 2000)),
        ("ticker_D_F/DE_2000_2009.txt", ("DE", 2000)),
        ("ticker_D_F/DBX_2010_2019.txt", ("DBX", 2010)),
        ("ticker_D_F/DECK_2000_2009.txt", ("DECK", 2000)),
        ("ticker_D_F/DB_2010_2019.txt", ("DB", 2010)),
        ("ticker_D_F/DECK_2010_2019.txt", ("DECK", 2010)),
        ("ticker_D_F/DE_2010_2019.txt", ("DE", 2010))
      )

    examples.foreach {
      case (name, sym) =>
        assertEquals(
          FirstRateData.symbolFromPath(null, name).unsafeRunSync,
          Option(sym)
        )
    }
  }
}
