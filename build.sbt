import com.typesafe.tools.mima.core.*
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject

import scala.scalanative.build.Mode

enablePlugins(ZioSbtEcosystemPlugin)

crossScalaVersions := Seq.empty

lazy val scala212 = "2.12.19"
lazy val scala213 = "2.13.14"
lazy val scala3   = "3.3.3"

lazy val scalaV    = scala213
lazy val allScalas = List("2.12", "2.13", "3.3")

inThisBuild(
  List(
    name         := "ZIO Query",
    zioVersion   := "2.1.8",
    scalaVersion := scalaV,
    developers := List(
      Developer(
        "kyri-petrou",
        "Kyri Petrou",
        "kyri.petrou@outlook.com",
        url("https://github.com/kyri-petrou")
      )
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
    zioQueryNative,
    docs
  )

lazy val zioQuery = crossProject(JSPlatform, JVMPlatform, NativePlatform)
  .enablePlugins(BuildInfoPlugin)
  .in(file("zio-query"))
  .settings(
    crossScalaVersions := List(scala212, scala213, scala3),
    stdSettings(
      name = Some("zio-query"),
      packageName = Some("zio.query"),
      enableCrossProject = true,
      turnCompilerWarningIntoErrors = true
    )
  )
  .settings(enableZIO())
  .settings(buildInfoSettings())
  .settings(scalacOptions += "-Wconf:msg=[zio.stacktracer.TracingImplicits.disableAutoTrace]:silent")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %%% "scala-collection-compat" % "2.12.0"
    ),
    scalacOptions ++=
      (if (scalaBinaryVersion.value == "3")
         Seq()
       else {
         Seq(
           "-opt:l:method",
           "-opt:l:inline",
           "-opt-inline-from:scala.**",
           "-opt-inline-from:zio.**"
         ) ++ (if (isRelease) Seq("-Xelide-below", "2001") else Seq())
       })
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

lazy val zioQueryNative = zioQuery.native
  .settings(
    scala3Settings,
    nativeConfig ~= { cfg =>
      val os = System.getProperty("os.name").toLowerCase
      // See https://github.com/zio/zio/releases/tag/v2.1.8
      if (os.contains("mac")) cfg.withMode(Mode.releaseFast)
      else cfg
    }
  )

lazy val zioQueryJVM = zioQuery.jvm.settings(enableMimaSettingsJVM)

lazy val benchmarks = project
  .in(file("benchmarks"))
  .settings(
    crossScalaVersions := List(scala213, scala3)
  )
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
    crossScalaVersions                         := Seq(scalaV),
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

lazy val ciReleaseModeKey = "CI_RELEASE_MODE"

lazy val isRelease = {
  val value = sys.env.contains(ciReleaseModeKey)
  if (value) println(s"Detected $ciReleaseModeKey envvar, enabling optimizations")
  value
}

def buildInfoSettings() =
  Seq(
    buildInfoObject := "BuildUtils",
    // BuildInfoOption.ConstantValue required to disable assertions in FiberRuntime!
    buildInfoOptions ++= Seq(BuildInfoOption.ConstantValue, BuildInfoOption.PackagePrivate),
    buildInfoKeys := Seq(
      BuildInfoKey("optimizationsEnabled" -> isRelease)
    ),
    buildInfoPackage := "zio.query"
  )
