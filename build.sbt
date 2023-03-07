import Versions._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject

enablePlugins(ZioSbtEcosystemPlugin, ZioSbtCiPlugin)

crossScalaVersions := Seq.empty

inThisBuild(
  List(
    name := "ZIO Query",
    zioVersion := "2.0.10",
    developers := List(
      Developer(
        "adamgfraser",
        "Adam Fraser",
        "adam.fraser@gmail.com",
        url("https://github.com/adamgfraser")
      )
    ),
    ciEnabledBranches := Seq("series/2.x"),
    supportedScalaVersions :=
      Map(
        (zioQueryJVM / thisProject).value.id -> (zioQueryJVM / crossScalaVersions).value,
        (zioQueryJS / thisProject).value.id  -> (zioQueryJS / crossScalaVersions).value
      )
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
      name = "zio-query",
      packageName = Some("zio.query"),
      enableSilencer = true,
      enableCrossProject = true
    )
  )
  .settings(enableZIO())
  .settings(
    scalacOptions ++= {
      if (scalaBinaryVersion.value == "3")
        Seq.empty
      else
        Seq("-P:silencer:globalFilters=[zio.stacktracer.TracingImplicits.disableAutoTrace]")
    }
  )

lazy val zioQueryJS = zioQuery.js
  .settings(
    scala3Settings,
    scalaJSUseMainModuleInitializer := true,
    crossScalaVersions -= scala211.value,
    scalacOptions ++= {
      if (scalaVersion.value == Scala3)
        Seq("-scalajs")
      else
        Seq.empty
    }
  )

lazy val zioQueryJVM = zioQuery.jvm

lazy val benchmarks = project
  .in(file("benchmarks"))
  .dependsOn(zioQueryJVM)
  .enablePlugins(JmhPlugin)

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
    projectStage                               := ProjectStage.Development,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(zioQueryJVM)
  )
  .dependsOn(zioQueryJVM)
  .enablePlugins(WebsitePlugin)
