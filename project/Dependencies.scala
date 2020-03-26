import sbt._

object Dependencies {
  val typelevel = "org.typelevel"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
  lazy val delimited = "net.tixxit" %% "delimited-core" % "0.9.5"
  lazy val cats = typelevel %% "cats-core" % "2.1.1"
  lazy val catsEffect = typelevel %% "cats-effect" % "2.1.2"
}
