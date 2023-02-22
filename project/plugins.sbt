val zioSbtVersion = "0.3.10+28-3aa9955a-SNAPSHOT"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-ci"        % zioSbtVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.6")

resolvers ++= Resolver.sonatypeOssRepos("public")
