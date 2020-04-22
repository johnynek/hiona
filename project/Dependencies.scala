import sbt._

object Dependencies {
  val typelevel = "org.typelevel"
  val aws2 = "software.amazon.awssdk"

  object V {
    val munit = "0.7.1"
    val aws2 = "2.11.14"
    val jawn = "1.0.0"
  }

  lazy val awsS3 = aws2 % "s3" % V.aws2
  lazy val awsLambdaCore1 = "com.amazonaws" % "aws-lambda-java-core" % "1.2.0"
  lazy val cats = typelevel %% "cats-core" % "2.1.1"
  lazy val catsCollections = typelevel %% "cats-collections-core" % "0.9.0"
  lazy val catsEffect = typelevel %% "cats-effect" % "2.1.2"
  lazy val decline = "com.monovore" %% "decline" % "1.0.0"
  lazy val delimited = "net.tixxit" %% "delimited-core" % "0.10.0"
  lazy val munit = "org.scalameta" %% "munit" % V.munit
  lazy val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % V.munit
  lazy val jawnParser = typelevel %% "jawn-parser" % V.jawn
  lazy val jawnAst = typelevel %% "jawn-ast" % V.jawn
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
}
