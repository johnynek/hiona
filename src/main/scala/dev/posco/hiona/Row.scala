package dev.posco.hiona

import cats.effect.{IO, Resource}
import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.nio.file.Path
import net.tixxit.delimited.{DelimitedFormat, Row => DRow}

import shapeless._

/**
  * typeclass for value A that are not allowed to have all empty strings
  * as values, and an efficient check to see if we are in the empty case
  * This exists to support optional values
  */
sealed trait NonEmptyRow[A] {
  def columns: Int
  def isMissing(offset: Int, s: DRow): Boolean
}

object NonEmptyRow extends Priority1NonEmptyRow {
  case class SingleNonEmpty[A]() extends NonEmptyRow[A] {
    def columns = 1
    def isMissing(offset: Int, s: DRow) = s(offset).isEmpty
  }

  implicit val boolNER: NonEmptyRow[Boolean] = SingleNonEmpty()
  implicit val byteNER: NonEmptyRow[Byte] = SingleNonEmpty()
  implicit val charNER: NonEmptyRow[Char] = SingleNonEmpty()
  implicit val shortNER: NonEmptyRow[Short] = SingleNonEmpty()
  implicit val intNER: NonEmptyRow[Int] = SingleNonEmpty()
  implicit val longNER: NonEmptyRow[Long] = SingleNonEmpty()
  implicit val floatNER: NonEmptyRow[Float] = SingleNonEmpty()
  implicit val doubleNER: NonEmptyRow[Double] = SingleNonEmpty()
  implicit val bigIntNER: NonEmptyRow[BigInt] = SingleNonEmpty()
  implicit val bigDecimalNER: NonEmptyRow[BigDecimal] = SingleNonEmpty()

  // a product is non-empty if any are non-empty
  case class HConsHead[A, B <: HList](rowA: NonEmptyRow[A], rowB: Row[B])
      extends NonEmptyRow[A :: B] {
    val columns = rowA.columns + rowB.columns
    def isMissing(offset: Int, s: DRow): Boolean =
      rowA.isMissing(offset, s)
  }

  // a product is non-empty if any are non-empty
  case class HConsTail[A, B <: HList](rowA: Row[A], rowB: NonEmptyRow[B])
      extends NonEmptyRow[A :: B] {
    val columns = rowA.columns + rowB.columns
    def isMissing(offset: Int, s: DRow): Boolean =
      rowB.isMissing(offset + rowA.columns, s)
  }

  // a coproduct is non-empty if all are non-empty
  case class CoprodNonEmpty[A, B <: Coproduct](
      neA: NonEmptyRow[A],
      neB: NonEmptyRow[B]
  ) extends NonEmptyRow[A :+: B] {
    val columns = neA.columns + neB.columns
    def isMissing(offset: Int, s: DRow): Boolean =
      neA.isMissing(offset, s) &&
        neB.isMissing(offset + neA.columns, s)
  }
}

sealed trait Priority1NonEmptyRow extends Priority2NonEmptyRow {
  // both aren't non-empty, head or tail may be
  implicit def hconsHeadNonEmpty[A, B <: HList](
      implicit rowA: NonEmptyRow[A],
      rowB: Row[B]
  ): NonEmptyRow[A :: B] =
    NonEmptyRow.HConsHead(rowA, rowB)

  // A Generic is provided by shapeless, and it can give a conversion from
  // case classes into HLists so this is what allows us to use case classes
  // for inputs and outputs.
  implicit def genericNonEmpty[A, B](
      implicit gen: Generic.Aux[A, B],
      rowB: NonEmptyRow[B]
  ): NonEmptyRow[A] =
    // NonEmptyRow never actually works with A or B, so the cast is safe:
    rowB.asInstanceOf[NonEmptyRow[A]]

  implicit def coprod1NonEmpty[A](
      implicit neA: NonEmptyRow[A]
  ): NonEmptyRow[A :+: CNil] =
    // this is always left, so we cast just like generic
    neA.asInstanceOf[NonEmptyRow[A :+: CNil]]
}

sealed trait Priority2NonEmptyRow {
  implicit def hconsTailNonEmpty[A, B <: HList](
      implicit rowA: Row[A],
      rowB: NonEmptyRow[B]
  ): NonEmptyRow[A :: B] =
    NonEmptyRow.HConsTail(rowA, rowB)

  implicit def coprod2NonEmpty[A, B <: Coproduct](
      implicit neA: NonEmptyRow[A],
      neB: NonEmptyRow[B]
  ): NonEmptyRow[A :+: B] =
    NonEmptyRow.CoprodNonEmpty(neA, neB)
}

/**
  * This is the typeclass-pattern which gives a serializer/deserializer for a type A
  * into and out of an Array[Strings]. This will be encoded into a CSV (or potentially TSV if we
  * needed, but that is currently not implemented)
  */
sealed trait Row[A] {
  // how many columns do we need to write A out
  def columns: Int
  // this writes A into an Array starting at a given offset
  def writeToStrings(a: A, offset: Int, dest: Array[String]): Unit
  // read A from a net.tixxit.delimited.Row (which is a wrapper for Array[String].
  // this may throw an exception, and should only be done inside a Try/IO call to
  // catch those exceptions.
  def unsafeFromStrings(offset: Int, s: DRow): A
}

object Row extends Priority1Rows {
  sealed abstract class Error(message: String) extends Exception(message) {
    def column: Int
    def columnText: String
  }

  case class DecodeFailure(column: Int, columnText: String, extra: String)
      extends Error(
        s"DecodeFailure in column: $column with <text>$columnText</text>. $extra"
      )

  implicit case object UnitRow extends Row[Unit] {
    def columns = 0
    def writeToStrings(a: Unit, offset: Int, dest: Array[String]) = ()
    def unsafeFromStrings(offset: Int, s: DRow) = ()
  }

  // a row we don't care about
  sealed trait Dummy
  final object Dummy extends Dummy

  implicit case object DummyRow extends Row[Dummy] {
    def columns = 1
    def writeToStrings(a: Dummy, offset: Int, dest: Array[String]) = ()
    def unsafeFromStrings(offset: Int, s: DRow) = Dummy
  }

  implicit case object StringRow extends Row[String] {
    def columns = 1
    def writeToStrings(a: String, offset: Int, dest: Array[String]) =
      dest(offset) = a
    def unsafeFromStrings(offset: Int, s: DRow): String =
      s(offset)
  }

  abstract class NumberRow[A](val typeName: String) extends Row[A] {
    def fromString(s: String): A
    def toString(a: A): String = a.toString

    private val msg = s"couldn't decode to $typeName"
    def columns = 1
    def writeToStrings(a: A, offset: Int, dest: Array[String]) =
      dest(offset) = toString(a)

    def unsafeFromStrings(offset: Int, s: DRow): A =
      try fromString(s(offset))
      catch {
        case (_: NumberFormatException) =>
          throw DecodeFailure(offset, s(offset), msg)
      }
  }

  implicit case object ByteRow extends NumberRow[Byte]("Byte") {
    def fromString(s: String) = s.toByte
  }

  implicit case object ShortRow extends NumberRow[Short]("Short") {
    def fromString(s: String) = s.toShort
  }

  implicit case object IntRow extends NumberRow[Int]("Int") {
    def fromString(s: String) = s.toInt
  }

  implicit case object LongRow extends NumberRow[Long]("Long") {
    def fromString(s: String) = s.toLong
  }

  implicit case object FloatRow extends NumberRow[Float]("Float") {
    def fromString(s: String) = s.toFloat
  }

  implicit case object DoubleRow extends NumberRow[Double]("Double") {
    def fromString(s: String) = s.toDouble
  }

  implicit case object BigIntRow extends NumberRow[BigInt]("BigInt") {
    def fromString(s: String) = BigInt(s)
  }

  implicit case object BigDecimalRow
      extends NumberRow[BigDecimal]("BigDecimal") {
    def fromString(s: String) =
      BigDecimal(new java.math.BigDecimal(s, java.math.MathContext.UNLIMITED))
  }

  implicit case object BooleanRow extends Row[Boolean] {
    def columns = 1
    def writeToStrings(a: Boolean, offset: Int, dest: Array[String]) =
      dest(offset) = a.toString
    def unsafeFromStrings(offset: Int, s: DRow): Boolean = {
      val str = s(offset)
      if (str == "true" || str == "TRUE" || str == "True") true
      else if (str == "false" || str == "FALSE" || str == "False") false
      else { throw DecodeFailure(offset, str, "could not decode boolean") }
    }
  }

  implicit case object CharRow extends Row[Char] {
    def columns = 1
    def writeToStrings(a: Char, offset: Int, dest: Array[String]) =
      dest(offset) = a.toString
    def unsafeFromStrings(offset: Int, s: DRow): Char = {
      val str = s(offset)
      if (str.length == 1) str.charAt(0)
      else sys.error(s"expected exactly one character at $offset, found: $str")
    }
  }

  /**
    * Optional data must not be empty to begin with. That is true for numbers and booleans
    * we could also support tuples/case-classes if needed, but currently that does not
    * work, only Option[Int], Option[Long], ... will work (notably, Option[String] won't work
    * since we can't tell Some("") from None).
    */
  implicit def optionRow[A](
      implicit rowA: Row[A],
      ner: NonEmptyRow[A]
  ): Row[Option[A]] =
    new Row[Option[A]] {
      val columns: Int = rowA.columns
      def writeToStrings(a: Option[A], offset: Int, dest: Array[String]): Unit =
        if (a.isDefined) {
          rowA.writeToStrings(a.get, offset, dest)
        } else {
          var idx = 0
          while (idx < columns) {
            dest(offset + idx) = ""
            idx += 1
          }
        }

      // may throw an Error
      def unsafeFromStrings(offset: Int, s: DRow): Option[A] = {
        // if all these are empty, we have None, else we decode
        val empty = ner.isMissing(offset, s)
        if (empty) None
        else Some(rowA.unsafeFromStrings(offset, s))
      }
    }

  /**
    * Helper function to make a Resource for a PrintWriter. The Resource
    * will close the PrintWriter when done.
    */
  def fileWriter(path: Path): Resource[IO, PrintWriter] =
    Resource.make(IO {
      val fw = new FileWriter(path.toFile)
      val bw = new BufferedWriter(fw)
      new PrintWriter(bw)
    })(pw => IO(pw.close()))

  /**
    * Make a Resource for a function that can write out items into a given path
    */
  def writerRes[A: Row](path: Path): Resource[IO, Iterable[A] => IO[Unit]] =
    fileWriter(path)
      .flatMap(pw => Resource.liftF(writer[A](pw)))

  private def writer[A: Row](pw: PrintWriter): IO[Iterable[A] => IO[Unit]] = {
    val format = DelimitedFormat.CSV

    val row = implicitly[Row[A]]

    IO {
      // use just a single buffer for this file
      val buffer = new Array[String](row.columns)

      { (items: Iterable[A]) =>
        IO {
          val iter = items.iterator
          while (iter.hasNext) {
            val a = iter.next()
            row.writeToStrings(a, 0, buffer)
            var idx = 0
            while (idx < buffer.length) {
              if (idx != 0) pw.print(format.separator)
              pw.print(format.render(buffer(idx)))
              idx += 1
            }
            pw.print(format.rowDelim.value)
          }
        }
      }
    }
  }

  case class Coproduct1Row[A](rowA: Row[A]) extends Row[A :+: CNil] {
    val columns = rowA.columns
    def writeToStrings(a: A :+: CNil, offset: Int, dest: Array[String]) =
      a match {
        case Inl(a) => rowA.writeToStrings(a, offset, dest)
        case Inr(u) => u.impossible
      }

    def unsafeFromStrings(offset: Int, s: DRow) =
      Inl(rowA.unsafeFromStrings(offset, s))
  }

  case class CoproductLeftNERow[A, B <: Coproduct](
      rowA: Row[A],
      neA: NonEmptyRow[A],
      rowB: Row[B]
  ) extends Row[A :+: B] {
    val columns = rowA.columns + rowB.columns
    def writeToStrings(ab: A :+: B, offset: Int, dest: Array[String]) =
      ab match {
        case Inl(a) =>
          rowA.writeToStrings(a, offset, dest)
          var idx = rowA.columns
          while (idx < columns) {
            dest(idx) = ""
            idx += 1
          }
        case Inr(b) =>
          var idx = 0
          while (idx < rowA.columns) {
            dest(idx) = ""
            idx += 1
          }
          rowB.writeToStrings(b, offset + rowA.columns, dest)
      }

    def unsafeFromStrings(offset: Int, s: DRow) =
      if (neA.isMissing(offset, s)) {
        Inr(rowB.unsafeFromStrings(offset + rowA.columns, s))
      } else Inl(rowA.unsafeFromStrings(offset, s))
  }

  case class CoproductRightNERow[A, B <: Coproduct](
      rowA: Row[A],
      rowB: Row[B],
      neB: NonEmptyRow[B]
  ) extends Row[A :+: B] {
    val columns = rowA.columns + rowB.columns
    def writeToStrings(ab: A :+: B, offset: Int, dest: Array[String]) =
      ab match {
        case Inl(a) =>
          rowA.writeToStrings(a, offset, dest)
          var idx = rowA.columns
          while (idx < columns) {
            dest(idx) = ""
            idx += 1
          }
        case Inr(b) =>
          var idx = 0
          while (idx < rowA.columns) {
            dest(idx) = ""
            idx += 1
          }
          rowB.writeToStrings(b, offset + rowA.columns, dest)
      }

    def unsafeFromStrings(offset: Int, s: DRow) =
      if (!neB.isMissing(offset + rowA.columns, s)) {
        Inr(rowB.unsafeFromStrings(offset + rowA.columns, s))
      } else Inl(rowA.unsafeFromStrings(offset, s))
  }

}

/**
  * This is using the fact that scala prefers implicit values in the direct class to superclasses
  * to prioritize which implicits we choose. Here we want to make instances of Row for genericRow and the hlist
  * (heterogenous lists, which are basically tuples that can be any size).
  */
sealed trait Priority1Rows extends Priority2Rows {
  implicit case object HNilRow extends Row[HNil] {
    def columns = 0
    def writeToStrings(a: HNil, offset: Int, dest: Array[String]) = ()
    def unsafeFromStrings(offset: Int, s: DRow) = HNil
  }

  case class HConsRow[A, B <: HList](rowA: Row[A], rowB: Row[B])
      extends Row[A :: B] {
    val columns = rowA.columns + rowB.columns
    def writeToStrings(ab: A :: B, offset: Int, dest: Array[String]) = {
      val (a :: b) = ab
      rowA.writeToStrings(a, offset, dest)
      rowB.writeToStrings(b, offset + rowA.columns, dest)
    }
    def unsafeFromStrings(offset: Int, s: DRow): A :: B = {
      val a = rowA.unsafeFromStrings(offset, s)
      val b = rowB.unsafeFromStrings(offset + rowA.columns, s)
      a :: b
    }
  }

  implicit def hconsRow[A, B <: HList](
      implicit rowA: Row[A],
      rowB: Row[B]
  ): Row[A :: B] =
    HConsRow(rowA, rowB)

  case class GenRow[A, B](gen: Generic.Aux[A, B], rowB: Row[B]) extends Row[A] {
    val columns = rowB.columns
    def writeToStrings(a: A, offset: Int, dest: Array[String]) =
      rowB.writeToStrings(gen.to(a), offset, dest)
    def unsafeFromStrings(offset: Int, s: DRow): A =
      gen.from(rowB.unsafeFromStrings(offset, s))
  }

  // A Generic is provided by shapeless, and it can give a conversion from
  // case classes into HLists so this is what allows us to use case classes
  // for inputs and outputs.
  implicit def genericRow[A, B](
      implicit gen: Generic.Aux[A, B],
      rowB: Row[B]
  ): Row[A] =
    GenRow(gen, rowB)

  implicit def cnil1Row[A](implicit rowA: Row[A]): Row[A :+: CNil] =
    Row.Coproduct1Row(rowA)
  implicit def coprodLeftRow[A, B <: Coproduct](
      implicit rowA: Row[A],
      neA: NonEmptyRow[A],
      rowB: Row[B]
  ): Row[A :+: B] =
    Row.CoproductLeftNERow(rowA, neA, rowB)
}

sealed trait Priority2Rows {
  implicit def coprodRightRow[A, B <: Coproduct](
      implicit rowA: Row[A],
      rowB: Row[B],
      neB: NonEmptyRow[B]
  ): Row[A :+: B] =
    Row.CoproductRightNERow(rowA, rowB, neB)
}
