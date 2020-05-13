import Dependencies._

ThisBuild / scalaVersion     := "2.13.2"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "dev.posco"
ThisBuild / organizationName := "devposco"

lazy val commonSettings =
  Seq(
    testFrameworks += new TestFramework("munit.Framework"),
    scalacOptions ++= Seq(
      "-Wunused",
      "-Xfatal-warnings",
      "-Xlint",
      "-Yrangepos",
      "-Ywarn-dead-code",
      "-Ywarn-value-discard",
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:existentials",
      "-language:experimental.macros",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-unchecked",
    ),
    // HACK: without these lines, the console is basically unusable,
    // since all imports are reported as being unused (and then become
    // fatal errors).
    scalacOptions in (Compile, console) ~= { _.filterNot("-Xlint" == _) },
    scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
  )

lazy val core = (project in file("core"))
  .settings(
    name := "hiona-core",
    moduleName := "hiona-core",
    libraryDependencies ++= Seq(
      munit % Test,
      munitScalaCheck % Test,
      scalaCheck % Test,
      cats,
      catsCollections,
      catsEffect,
      delimited,
      decline,
      doobie,
      diffx,
      fs2,
      fs2io,
      shapeless,
    ),
    commonSettings,
  )

lazy val aws = (project in file("aws"))
  .settings(
    name := "hiona-aws",
    moduleName := "hiona-aws",
    libraryDependencies ++= Seq(
      munit % Test,
      munitScalaCheck % Test,
      scalaCheck % Test,
      awsS3v1,
      awsLambdaCore1,
      awsSecretsManager,
      fs2,
      fs2io,
      jawnAst,
      jawnParser,
      postgresJdbc,
      streamUpload,
    ),
    commonSettings,
  )
  .dependsOn(core)

lazy val jobs = (project in file("jobs"))
  .settings(
    name := "hiona-jobs",
    moduleName := "hiona-jobs",
    libraryDependencies ++= Seq(
      munit % Test,
      munitScalaCheck % Test,
      scalaCheck % Test,
    ),
    commonSettings,
  )
  .dependsOn(core, aws)

lazy val bench = (project in file("bench"))
  .enablePlugins(JmhPlugin)
  .settings(
    name := "hiona-bench",
    moduleName := "hiona-bench",
    commonSettings,
  )
  .dependsOn(core)
