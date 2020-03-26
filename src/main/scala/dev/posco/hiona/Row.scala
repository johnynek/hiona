package dev.posco.hiona

// read/write CSV/TSV
sealed trait Row[A] {
  def columns: Int
  def writeToStrings(a: A, offset: Int, dest: Array[String]): Unit
  def fromStrings(s: Seq[String]): Option[A]
  def zip[B](that: Row[B]): Row[(A, B)] = ???
  def imap[B](toFn: A => Option[B])(fromFn: B => A): Row[B] = ???
}

object Row {
  // we know that at least one of columns is non-empty for all values
  sealed trait NonEmptyRow[A] extends Row[A]

  implicit case object UnitRow extends Row[Unit] {
    val someUnit: Some[Unit] = Some(())

    def columns = 0
    def writeToStrings(a: Unit, offset: Int, dest: Array[String]) = ()
    def fromStrings(s: Seq[String]): Option[Unit] = someUnit
  }
}

