import Dependencies._

ThisBuild / scalaVersion     := "2.12.11"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "hiona",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      cats,
      catsEffect,
      delimited,
      decline,
      shapeless,
    ),
    scalacOptions += "-Ypartial-unification",
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
