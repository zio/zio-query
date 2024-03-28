val zioSbtVersion = "0.4.0-alpha.25"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-ci"        % zioSbtVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage"   % "2.0.11")
addSbtPlugin("com.typesafe"  % "sbt-mima-plugin" % "1.1.3")

resolvers ++= Resolver.sonatypeOssRepos("public")
