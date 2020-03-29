import sbt._

object Dependencies {
  val typelevel = "org.typelevel"

  object V {
    val munit = "0.7.1"
  }

  lazy val cats = typelevel %% "cats-core" % "2.1.1"
  lazy val catsEffect = typelevel %% "cats-effect" % "2.1.2"
  lazy val decline = "com.monovore" %% "decline" % "1.0.0"
  lazy val delimited = "net.tixxit" %% "delimited-core" % "0.9.5"
  lazy val munit = "org.scalameta" %% "munit" % V.munit
  lazy val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % V.munit
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
}
