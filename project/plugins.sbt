val ZioSbtVersion = "0.3.10+25-abf5354a-SNAPSHOT"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % ZioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % ZioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-ci"        % ZioSbtVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.6")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.5"

resolvers ++= Resolver.sonatypeOssRepos("public")
