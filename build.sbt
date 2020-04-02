import Dependencies._

ThisBuild / scalaVersion     := "2.13.1"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "hiona",
    testFrameworks += new TestFramework("munit.Framework"),
    libraryDependencies ++= Seq(
      munit % Test,
      munitScalaCheck % Test,
      scalaCheck % Test,
      cats,
      catsCollections,
      catsEffect,
      delimited,
      decline,
      shapeless,
    ),
    // needed in 2.11 and 2.12
    //scalacOptions += "-Ypartial-unification",
    // turns on the optimizer, slower to compile, but seems to give a slight improvement
    //scalacOptions ++= Seq("-opt:l:inline", "'-opt-inline-from:**'"),
    scalacOptions ++= Seq("-Wunused", "-Yrangepos"),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  )
