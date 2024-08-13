val zioSbtVersion = "0.4.0-alpha.28"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-ci"        % zioSbtVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage"   % "2.1.0")
addSbtPlugin("com.typesafe"  % "sbt-mima-plugin" % "1.1.4")
addSbtPlugin("com.eed3si9n"  % "sbt-buildinfo"   % "0.12.0")

resolvers ++= Resolver.sonatypeOssRepos("public")
