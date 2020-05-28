package dev.posco.hiona

final class LazyToString(build: () => Any) {
  private[this] lazy val result = build()

  override def toString = result.toString
  override def hashCode = result.hashCode
  override def equals(that: Any) = result.equals(that)
}

object LazyToString {
  def apply(any: => Any): LazyToString = new LazyToString(() => any)
}
