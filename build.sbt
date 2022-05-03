import Dependencies._

ThisBuild / tlBaseVersion := "0.1" // your current series x.y

ThisBuild / organization := "dev.posco"
ThisBuild / organizationName := "devposco"
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  tlGitHubDev("johnynek", "P. Oscar Boykin")
)

// set to false to publish to s01.oss.sonatype.org
ThisBuild / tlSonatypeUseLegacyHost := true

val Scala212 = "2.12.15"
val Scala213 = "2.13.3"
ThisBuild / crossScalaVersions := Seq(
  Scala213
) // Ready to support other Scala versions
ThisBuild / scalaVersion := Scala213

def scalaVersionSpecificFolders(
    srcName: String,
    srcBaseDir: java.io.File,
    scalaVersion: String
) = {
  def extraDirs(suffix: String) =
    List(CrossType.Pure, CrossType.Full)
      .flatMap(
        _.sharedSrcDir(srcBaseDir, srcName).toList.map(f =>
          file(f.getPath + suffix)
        )
      )
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, y)) if y <= 12 =>
      extraDirs("-2.12-")
    case Some((2, y)) if y >= 13 =>
      extraDirs("-2.13+")
    case Some((3, _)) =>
      extraDirs("-2.13+")
    case _ => Nil
  }
}

// Setup coverage
ThisBuild / githubWorkflowAddedJobs +=
  WorkflowJob(
    id = "coverage",
    name = "Generate coverage report",
    scalas = List(Scala213),
    steps = List(WorkflowStep.Checkout) ++ WorkflowStep.SetupJava(
      githubWorkflowJavaVersions.value.toList
    ) ++ githubWorkflowGeneratedCacheSteps.value ++ List(
      WorkflowStep.Sbt(List("coverage", "root/test", "coverageAggregate")),
      WorkflowStep.Run(List("bash <(curl -s https://codecov.io/bash)"))
    )
  )

ThisBuild / tlCiReleaseBranches := Seq("master")
ThisBuild / tlSitePublishBranch := Some("master")

lazy val root = tlCrossRootProject.aggregate(core, jobs, bench, aws)

// We keep JVM specific settings separate in case we want to support JS and Native later
lazy val commonJvmSettings = Seq(
  Test / testOptions += Tests.Argument(TestFrameworks.MUnit)
)

lazy val commonSettings = Seq(
  scalacOptions ++= (
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n <= 12 =>
        Seq(
          "-Xfatal-warnings",
          "-Yno-adapted-args",
          "-Xfuture"
        )
      case _ =>
        Nil
    }
  ),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _                                   => MergeStrategy.first
  },
  addCompilerPlugin(
    "org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full
  ),
  Compile / unmanagedSourceDirectories ++= scalaVersionSpecificFolders(
    "main",
    baseDirectory.value,
    scalaVersion.value
  ),
  Test / unmanagedSourceDirectories ++= scalaVersionSpecificFolders(
    "test",
    baseDirectory.value,
    scalaVersion.value
  )
) ++ commonJvmSettings

lazy val core = project
  .in(file("core"))
  .enablePlugins(NoPublishPlugin)
  .settings(
    name := "hiona-core",
    moduleName := "hiona-core",
    libraryDependencies ++= Seq(
      algebird,
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
      circeFs2,
      shapeless,
      slf4jApi
    ),
    commonSettings
  )

lazy val IntegrationTest = config("it").extend(Test)

lazy val aws = project
  .in(file("aws"))
  .configs(IntegrationTest)
  .enablePlugins(NoPublishPlugin)
  .settings(
    Defaults.itSettings,
    name := "hiona-aws",
    moduleName := "hiona-aws",
    libraryDependencies ++= Seq(
      munit % "it,test",
      munitScalaCheck % "it,test",
      scalaCheck % "it,test",
      h2 % Test,
      awsLambdaCore1,
      awsLambdaService,
      awsEcsService,
      awsEcrService,
      awsS3v1,
      awsSecretsManager,
      circeCore,
      circeGeneric,
      circeJawn,
      circeParser,
      commonsCodec,
      doobieHikari,
      fs2,
      fs2io,
      postgresJdbc,
      streamUpload
    ),
    commonSettings
  )
  .dependsOn(core)

lazy val jobs = project
  .in(file("jobs"))
  .enablePlugins(NoPublishPlugin)
  .settings(
    name := "hiona-jobs",
    moduleName := "hiona-jobs",
    libraryDependencies ++= Seq(
      algebird,
      munit % Test,
      munitScalaCheck % Test,
      scalaCheck % Test,
      slf4jSimple
    ),
    commonSettings
  )
  .dependsOn(core, aws)

lazy val bench = project
  .in(file("bench"))
  .dependsOn(core)
  .enablePlugins(NoPublishPlugin)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "hiona-bench",
    moduleName := "hiona-bench",
    commonSettings
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(TypelevelPlugin)
  .settings(
    name := "hiona-docs"
  )
