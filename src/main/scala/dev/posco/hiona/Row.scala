package dev.posco.hiona

import cats.effect.{IO, Resource}
import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.nio.file.Path
import net.tixxit.delimited.{ DelimitedFormat, Row => DRow}

import shapeless._

// read/write CSV/TSV
sealed trait Row[A] {
  def columns: Int
  def writeToStrings(a: A, offset: Int, dest: Array[String]): Unit
  // may throw an Error
  def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): A
}

object Row extends Priority1Rows {
  sealed abstract class Error(message: String) extends Exception(message) {
    def column: Int
    def columnText: String
  }

  case class DecodeFailure(
    column: Int,
    columnText: String,
    extra: String) extends Error(s"DecodeFailure in column: $column with <text>$columnText</text>. $extra")

  // we know that at least one of columns is non-empty for all values
  sealed trait NonEmptyRow[A] extends Row[A]

  implicit case object UnitRow extends Row[Unit] {
    def columns = 0
    def writeToStrings(a: Unit, offset: Int, dest: Array[String]) = ()
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]) = ()
  }

  // a row we don't care about
  sealed trait Dummy
  final object Dummy extends Dummy

  implicit case object DummyRow extends Row[Dummy] {
    def columns = 1
    def writeToStrings(a: Dummy, offset: Int, dest: Array[String]) = ()
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]) = Dummy
  }

  implicit case object StringRow extends Row[String] {
    def columns = 1
    def writeToStrings(a: String, offset: Int, dest: Array[String]) = {
      dest(offset) = a
    }
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): String =
      s(offset)
  }

  abstract class NumberRow[A] extends NonEmptyRow[A] {
    val typeName: String
    def fromString(s: String): A
    def toString(a: A): String

    private val msg = s"couldn't decode to $typeName"
    def columns = 1
    def writeToStrings(a: A, offset: Int, dest: Array[String]) = {
      dest(offset) = toString(a)
    }

    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): A =
      try fromString(s(offset))
      catch {
        case (_: NumberFormatException) => throw DecodeFailure(offset, s(offset), msg)
      }
  }

  implicit case object IntRow extends NumberRow[Int] {
    val typeName = "Int"
    def fromString(s: String) = s.toInt
    def toString(i: Int) = i.toString
  }

  implicit case object LongRow extends NumberRow[Long] {
    val typeName = "Long"
    def fromString(s: String) = s.toLong
    def toString(i: Long) = i.toString
  }

  implicit case object FloatRow extends NumberRow[Float] {
    val typeName = "Float"
    def fromString(s: String) = s.toFloat
    def toString(i: Float) = i.toString
  }

  implicit case object DoubleRow extends NumberRow[Double] {
    val typeName = "Double"
    def fromString(s: String) = s.toDouble
    def toString(i: Double) = i.toString
  }

  implicit case object BigIntRow extends NumberRow[BigInt] {
    val typeName = "BigInt"
    def fromString(s: String) = BigInt(s)
    def toString(i: BigInt) = i.toString
  }

  implicit case object BigDecimalRow extends NumberRow[BigDecimal] {
    val typeName = "BigDecimal"
    def fromString(s: String) =
      BigDecimal(new java.math.BigDecimal(s, java.math.MathContext.UNLIMITED))
    def toString(i: BigDecimal) = i.toString
  }

  implicit case object BooleanRow extends NonEmptyRow[Boolean] {
    def columns = 1
    def writeToStrings(a: Boolean, offset: Int, dest: Array[String]) = {
      dest(offset) = a.toString
    }
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): Boolean = {
      val str = s(offset)
      if (str == "true" || str == "TRUE" || str == "True") true
      else if (str == "false" || str == "FALSE" || str == "False") false
      else { throw DecodeFailure(offset, str, "could not decode boolean") }
    }
  }

  implicit def optionRow[A](implicit rowA: NonEmptyRow[A]): Row[Option[A]] =
    new Row[Option[A]] {
      val columns: Int = rowA.columns
      def writeToStrings(a: Option[A], offset: Int, dest: Array[String]): Unit =
        if (a.isDefined) {
          rowA.writeToStrings(a.get, offset, dest)
        }
        else ()

      // may throw an Error
      def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): Option[A] = {
        // if all these are empty, we have None, else we decode
        var empty = true
        var idx = 0
        while (empty && (idx < columns)) {
          empty = s(idx + offset).isEmpty
          idx += 1
        }

        if (empty) None
        else Some(rowA.unsafeFromStrings(offset, s))
      }
    }

  def fileWriter(path: Path): Resource[IO, PrintWriter] =
    Resource.make(IO {
      val fw = new FileWriter(path.toFile)
      val bw = new BufferedWriter(fw)
      new PrintWriter(bw)
    }) { pw => IO(pw.close()) }

  def writerRes[A: Row](path: Path): Resource[IO, Iterable[A] => IO[Unit]] =
    fileWriter(path)
      .flatMap { pw =>
        Resource.liftF(writer[A](pw))
      }

  private def writer[A: Row](pw: PrintWriter): IO[Iterable[A] => IO[Unit]] = {
    val format = DelimitedFormat.CSV

    val row = implicitly[Row[A]]

    IO {
      // use just a single buffer for this file
      val buffer = new Array[String](row.columns)

      { (items: Iterable[A]) =>
        val iter = items.iterator

        IO {
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
}

sealed trait Priority1Rows {
  implicit case object HNilRow extends Row[HNil] {
    def columns = 0
    def writeToStrings(a: HNil, offset: Int, dest: Array[String]) = ()
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]) = HNil
  }

  case class HConsRow[A, B <: HList](rowA: Row[A], rowB: Row[B]) extends Row[A :: B] {
    val columns = rowA.columns + rowB.columns
    def writeToStrings(ab: A :: B, offset: Int, dest: Array[String]) = {
      val (a :: b) = ab
      rowA.writeToStrings(a, offset, dest)
      rowB.writeToStrings(b, offset + rowA.columns, dest)
    }
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): A :: B = {
      val a = rowA.unsafeFromStrings(offset, s)
      val b = rowB.unsafeFromStrings(offset + rowA.columns, s)
      a :: b
    }
  }

  implicit def hconsRow[A, B <: HList](implicit rowA: Row[A], rowB: Row[B]): Row[A :: B] =
    HConsRow(rowA, rowB)

  case class GenRow[A, B](gen: Generic.Aux[A, B], rowB: Row[B]) extends Row[A] {
    val columns = rowB.columns
    def writeToStrings(a: A, offset: Int, dest: Array[String]) = {
      rowB.writeToStrings(gen.to(a), offset, dest)
    }
    def unsafeFromStrings(offset: Int, s: IndexedSeq[String]): A =
      gen.from(rowB.unsafeFromStrings(offset, s))
  }

  implicit def genericRow[A, B](implicit gen: Generic.Aux[A, B], rowB: Row[B]): Row[A] =
    GenRow(gen, rowB)
}
