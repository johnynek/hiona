package dev.posco.hiona

import org.scalacheck.{Arbitrary, Prop}
import org.scalacheck.Prop.forAll
import net.tixxit.delimited.{Row => DRow}

// make generators for case classes
import ShapelessGen._

class RowLaws extends munit.ScalaCheckSuite {

  def testRow[A: Row: Arbitrary](cols: Int): Prop = {
    val row = implicitly[Row[A]]

    val pser = forAll { (a: A) =>
      val ary = new Array[String](row.columns)
      row.writeToStrings(a, 0, ary)
      val drow = DRow.fromArray(ary)
      val a1 = row.unsafeFromStrings(0, drow)
      assertEquals(a1, a)
    }.label("serde test")

    val pcols =
      Prop(row.columns == cols).label(s"columns match: ${row.columns} == $cols")

    pser && pcols
  }

  property("Unit row")(testRow[Unit](0))
  property("Byte row")(testRow[Byte](1))
  property("Short row")(testRow[Short](1))
  property("Char row")(testRow[Char](1))
  property("Int row")(testRow[Int](1))
  property("Long row")(testRow[Long](1))
  property("Float row")(testRow[Float](1))
  property("Double row")(testRow[Double](1))
  property("(Int, Short) row")(testRow[(Int, Short)](2))
  property("Either[Unit, Boolean] row")(testRow[Either[Unit, Boolean]](1))
  property("Either[Boolean, Unit] row")(testRow[Either[Boolean, Unit]](1))
  property("Option[(Int, Short)] row")(testRow[Option[(Int, Short)]](2))
  property("Option[(Int, Option[Short])] row") {
    testRow[Option[(Int, Option[Short])]](2)
  }
  property("Either[Int, String]")(testRow[Either[Int, String]](2))
  property("Either[Int, Option[Int]]")(testRow[Either[Int, Option[Int]]](2))

  // Option[String] isn't supported
  shapeless.test.illTyped("""implicitly[Row[Option[String]]]""")
  // Option[(String, Boolean)] is supported
  property("Option[(String, Boolean)]")(testRow[Option[(String, Boolean)]](2))

  case class Foo(x: Int, str: String, opt: Option[BigInt])
  property("Foo")(testRow[Foo](3))

  sealed trait Bar
  object Bar {
    case object Bar0 extends Bar
    case class Bar1(int: Int) extends Bar
    case class Bar2(num: Double) extends Bar
  }

  property("Bar")(testRow[Bar](2))

  def testValue[A: Row](a: A, ser: Seq[String]) = {
    val row = implicitly[Row[A]]
    val ary = new Array[String](row.columns)
    row.writeToStrings(a, 0, ary)
    assertEquals(DRow.fromArray(ary), DRow.fromArray(ser.toArray))
    val a1 = row.unsafeFromStrings(0, DRow.fromArray(ary))
    assertEquals(a1, a)
  }

  test("some specific values") {
    testValue((), List())
    testValue(true, List("true"))
    testValue[Either[Int, String]](Right("foo"), List("", "foo"))
    testValue[Either[Int, String]](Left(42), List("42", ""))

    testValue[Option[(Int, Option[Short])]](None, List("", ""))
    testValue[Option[(Int, Option[Short])]](Some((1, None)), List("1", ""))
    testValue[Option[(Int, Option[Short])]](
      Some((1, Some(42.toShort))),
      List("1", "42")
    )

    testValue[Bar](Bar.Bar0, List("", ""))
    testValue[Bar](Bar.Bar1(1), List("1", ""))
    testValue[Bar](Bar.Bar2(2.0), List("", "2.0"))
  }
}
