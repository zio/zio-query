import BuildHelper._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject

name := "zio-query"

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.github.io/zio-query/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "adamgfraser",
        "Adam Fraser",
        "adam.fraser@gmail.com",
        url("https://github.com/adamgfraser")
      )
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion = "2.0.0-RC3"

lazy val root = project
  .in(file("."))
  .settings(
    publish / skip := true,
    unusedCompileDependenciesFilter -= moduleFilter("org.scala-js", "scalajs-library")
  )
  .aggregate(
    zioQueryJVM,
    zioQueryJS
  )

lazy val zioQuery = crossProject(JSPlatform, JVMPlatform)
  .in(file("zio-query"))
  .settings(stdSettings("zio-query"))
  .settings(crossProjectSettings)
  .settings(buildInfoSettings("zio.query"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % "test",
      "dev.zio" %% "zio-test-sbt" % zioVersion % "test"
    )
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .settings(
    scalacOptions ++= {
      if (scalaVersion.value == ScalaDotty)
        Seq.empty
      else
        Seq("-P:silencer:globalFilters=[zio.stacktracer.TracingImplicits.disableAutoTrace]")
    }
  )

lazy val zioQueryJS = zioQuery.js
  .settings(dottySettings)
  .settings(scalaJSUseMainModuleInitializer := true)

lazy val zioQueryJVM = zioQuery.jvm
  .settings(dottySettings)

lazy val docs = project
  .in(file("zio-query-docs"))
  .settings(
    publish / skip := true,
    moduleName := "zio-query-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    ),
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(root),
    ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .dependsOn(zioQueryJVM)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
