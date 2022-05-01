import sbt._

object Dependencies {
  val typelevel = "org.typelevel"
  val aws1 = "com.amazonaws"

  object V {
    val munit = "0.7.7"
    val circe = "0.13.0"
    val fs2 = "2.4.2"
    val slf4j = "1.7.30"
  }

  lazy val algebird = "com.twitter" %% "algebird-core" % "0.13.7"
  lazy val awsLambdaCore1 = "com.amazonaws" % "aws-lambda-java-core" % "1.2.0"
  lazy val awsLambdaService =
    "com.amazonaws" % "aws-java-sdk-lambda" % "1.11.783"
  lazy val awsEcsService = "com.amazonaws" % "aws-java-sdk-ecs" % "1.11.809"
  lazy val awsEcrService = "com.amazonaws" % "aws-java-sdk-ecr" % "1.11.809"
  lazy val awsS3v1 = aws1 % "aws-java-sdk-s3" % "1.11.766"
  lazy val awsSecretsManager =
    "com.amazonaws" % "aws-java-sdk-secretsmanager" % "1.11.774"
  lazy val cats = typelevel %% "cats-core" % "2.1.1"
  lazy val catsCollections = typelevel %% "cats-collections-core" % "0.9.0"
  lazy val catsEffect = typelevel %% "cats-effect" % "2.1.2"
  lazy val circeCore = "io.circe" %% "circe-core" % V.circe
  lazy val circeGeneric = "io.circe" %% "circe-generic" % V.circe
  lazy val circeJawn = "io.circe" %% "circe-jawn" % V.circe
  lazy val circeParser = "io.circe" %% "circe-parser" % V.circe
  lazy val commonsCodec = "commons-codec" % "commons-codec" % "1.14"
  lazy val decline = "com.monovore" %% "decline" % "1.0.0"
  lazy val delimited = "net.tixxit" %% "delimited-core" % "0.10.0"
  lazy val diffx = "com.softwaremill.diffx" %% "diffx-core" % "0.3.28"
  lazy val doobie = "org.tpolecat" %% "doobie-core" % "0.9.0"
  lazy val doobieHikari = "org.tpolecat" %% "doobie-hikari" % "0.9.0"
  lazy val fs2 = "co.fs2" %% "fs2-core" % V.fs2
  lazy val fs2io = "co.fs2" %% "fs2-io" % V.fs2
  lazy val h2 = "com.h2database" % "h2" % "1.4.200"
  lazy val munit = "org.scalameta" %% "munit" % V.munit
  lazy val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % V.munit
  lazy val postgresJdbc = "org.postgresql" % "postgresql" % "42.2.12"
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
  lazy val slf4jApi = "org.slf4j" % "slf4j-api" % V.slf4j
  lazy val slf4jSimple = "org.slf4j" % "slf4j-simple" % V.slf4j
  lazy val streamUpload = "com.github.alexmojaki" % "s3-stream-upload" % "2.2.0"
}
