val zioSbtVersion = "0.3.10+33-b66126a5-SNAPSHOT"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-ci"        % zioSbtVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.6")

resolvers ++= Resolver.sonatypeOssRepos("public")
