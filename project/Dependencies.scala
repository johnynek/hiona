import sbt._

object Dependencies {
  val typelevel = "org.typelevel"
  val aws1 = "com.amazonaws"

  object V {
    val munit = "0.7.7"
    val jawn = "1.0.0"
    val fs2 = "2.3.0"
  }

  lazy val awsLambdaCore1 = "com.amazonaws" % "aws-lambda-java-core" % "1.2.0"
  lazy val awsSecretsManager = "com.amazonaws" % "aws-java-sdk-secretsmanager" % "1.11.774"
  lazy val awsS3v1 = aws1 % "aws-java-sdk-s3" % "1.11.766"
  lazy val cats = typelevel %% "cats-core" % "2.1.1"
  lazy val catsCollections = typelevel %% "cats-collections-core" % "0.9.0"
  lazy val catsEffect = typelevel %% "cats-effect" % "2.1.2"
  lazy val decline = "com.monovore" %% "decline" % "1.0.0"
  lazy val delimited = "net.tixxit" %% "delimited-core" % "0.10.0"
  lazy val diffx = "com.softwaremill.diffx" %% "diffx-core" % "0.3.28"
  lazy val doobie = "org.tpolecat" %% "doobie-core" % "0.9.0"
  lazy val fs2 = "co.fs2" %% "fs2-core" % V.fs2
  lazy val fs2io = "co.fs2" %% "fs2-io" % V.fs2
  lazy val munit = "org.scalameta" %% "munit" % V.munit
  lazy val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % V.munit
  lazy val jawnParser = typelevel %% "jawn-parser" % V.jawn
  lazy val jawnAst = typelevel %% "jawn-ast" % V.jawn
  lazy val postgresJdbc = "org.postgresql" % "postgresql" % "42.2.12"
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
  lazy val streamUpload = "com.github.alexmojaki" % "s3-stream-upload" % "2.2.0"
}
