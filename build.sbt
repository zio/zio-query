import Versions._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject

enablePlugins(ZioSbtEcosystemPlugin, ZioSbtCiPlugin)

inThisBuild(
  List(
    name               := "zio-query",
    scalaVersion       := Scala213,
    crossScalaVersions := Seq(Scala211, Scala212, Scala213, Scala3),
    organization       := "dev.zio",
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

//addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
//addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion = "2.0.10"

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
  .settings(stdSettings(name = "zio-query", packageName = "zio.query", enableSilencer = true))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    )
  )
  .settings(enableZIO(zioVersion, enableTesting = true))
  .settings(
    scalacOptions ++= {
      if (scalaVersion.value == Scala3)
        Seq.empty
      else
        Seq("-P:silencer:globalFilters=[zio.stacktracer.TracingImplicits.disableAutoTrace]")
    }
  )

lazy val zioQueryJS = zioQuery.js
  .settings(scalaJSUseMainModuleInitializer := true)
  .settings(
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
    projectName    := "ZIO Query",
    mainModuleName := (zioQueryJVM / moduleName).value,
    crossScalaVersions -= Scala211,
    projectStage                               := ProjectStage.Development,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(zioQueryJVM)
  )
  .dependsOn(zioQueryJVM)
  .enablePlugins(WebsitePlugin)
