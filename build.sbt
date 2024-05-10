import com.typesafe.tools.mima.core.*
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject
import zio.sbt.githubactions.*
import _root_.io.circe.syntax.*

enablePlugins(ZioSbtEcosystemPlugin, ZioSbtCiPlugin)

crossScalaVersions := Seq.empty

inThisBuild(
  List(
    name       := "ZIO Query",
    zioVersion := "2.1.0",
    developers := List(
      Developer(
        "kyri-petrou",
        "Kyri Petrou",
        "kyri.petrou@outlook.com",
        url("https://github.com/kyri-petrou")
      )
    ),
    ciEnabledBranches := Seq("series/2.x"),
    ciTargetScalaVersions :=
      Map(
        (zioQueryJVM / thisProject).value.id -> (zioQueryJVM / crossScalaVersions).value,
        (zioQueryJS / thisProject).value.id  -> (zioQueryJS / crossScalaVersions).value
      ),
    versionScheme := Some("early-semver")
  )
)

lazy val root = project
  .in(file("."))
  .settings(
    publish / skip := true,
    unusedCompileDependenciesFilter -= moduleFilter("org.scala-js", "scalajs-library")
  )
  .aggregate(
    zioQueryJVM,
    zioQueryJS,
    docs
  )

lazy val zioQuery = crossProject(JSPlatform, JVMPlatform)
  .in(file("zio-query"))
  .settings(
    stdSettings(
      name = Some("zio-query"),
      packageName = Some("zio.query"),
      enableCrossProject = true
    )
  )
  .settings(enableZIO())
  .settings(scalacOptions += "-Wconf:msg=[zio.stacktracer.TracingImplicits.disableAutoTrace]:silent")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0"
    ),
    scalacOptions ++=
      (if (scalaBinaryVersion.value == "3")
         Seq()
       else
         Seq(
           "-opt:l:method",
           "-opt:l:inline",
           "-opt-inline-from:scala.**"
         ))
  )

lazy val zioQueryJS = zioQuery.js
  .settings(enableMimaSettingsJS)
  .settings(
    scala3Settings,
    scalaJSUseMainModuleInitializer := true,
    scalacOptions ++= {
      if (scalaBinaryVersion.value == "3")
        Seq("-scalajs")
      else
        Seq.empty
    }
  )

lazy val zioQueryJVM = zioQuery.jvm.settings(enableMimaSettingsJVM)

lazy val benchmarks = project
  .in(file("benchmarks"))
  .dependsOn(zioQueryJVM)
  .enablePlugins(JmhPlugin)
  .settings(libraryDependencies += "com.47deg" %% "fetch" % "3.1.2")

lazy val docs = project
  .in(file("zio-query-docs"))
  .settings(
    moduleName := "zio-query-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    projectName                                := (ThisBuild / name).value,
    mainModuleName                             := (zioQueryJVM / moduleName).value,
    scalaVersion                               := scala213.value,
    crossScalaVersions                         := Seq(scala213.value),
    projectStage                               := ProjectStage.ProductionReady,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(zioQueryJVM)
  )
  .dependsOn(zioQueryJVM)
  .enablePlugins(WebsitePlugin)

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val enforceMimaCompatibility = true // Enable / disable failing CI on binary incompatibilities

lazy val enableMimaSettingsJVM =
  Def.settings(
    mimaFailOnProblem     := enforceMimaCompatibility,
    mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% moduleName.value % _).toSet,
    mimaBinaryIssueFilters ++= Seq()
  )

lazy val enableMimaSettingsJS =
  Def.settings(
    mimaFailOnProblem     := enforceMimaCompatibility,
    mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %%% moduleName.value % _).toSet,
    mimaBinaryIssueFilters ++= Seq()
  )

ThisBuild / ciCheckArtifactsBuildSteps +=
  Step.SingleStep(
    "Check binary compatibility",
    run = Some("sbt \"+zioQueryJVM/mimaReportBinaryIssues; +zioQueryJS/mimaReportBinaryIssues\"")
  )

// Temporary, until zio-test issue with ScalaJS 2.12 is resolved
ThisBuild / ciTestJobs := Seq(
  Job(
    id = "test",
    name = "Test",
    runsOn = "ubuntu-latest",
    timeoutMinutes = 15,
    continueOnError = false,
    strategy = Some(
      Strategy(
        matrix = Map(
          "java" -> List("11", "21"),
          "scala-project" -> List(
            "++2.12 zioQueryJVM",
            "++2.13 zioQueryJVM",
            "++3.3 zioQueryJVM",
            "++2.13 zioQueryJS",
            "++3.3 zioQueryJS"
          )
        ),
        failFast = false
      )
    ),
    steps = List(
      Step.SingleStep(
        name = "Setup Scala",
        uses = Some(ActionRef("actions/setup-java@v4")),
        parameters = Map(
          "distribution" -> "corretto".asJson,
          "java-version" -> "${{ matrix.java }}".asJson,
          "check-latest" -> true.asJson
        )
      ),
      Step.SingleStep(
        name = "Cache Dependencies",
        uses = Some(ActionRef("coursier/cache-action@v6"))
      ),
      Step.SingleStep(
        name = "Git Checkout",
        uses = Some(ActionRef("actions/checkout@v4")),
        parameters = Map(
          "fetch-depth" -> "0".asJson
        )
      ),
      Step.SingleStep(
        name = "Test",
        run = Some("sbt ${{ matrix.scala-project }}/test")
      )
    )
  )
)
