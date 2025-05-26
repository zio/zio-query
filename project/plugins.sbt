val zioSbtVersion = "0.4.0-alpha.31"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)

addSbtPlugin("org.scoverage"    % "sbt-scoverage"    % "2.3.1")
addSbtPlugin("com.typesafe"     % "sbt-mima-plugin"  % "1.1.4")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"    % "0.13.1")
addSbtPlugin("org.scala-native" % "sbt-scala-native" % "0.5.7")
addSbtPlugin("org.scala-js"     % "sbt-scalajs"      % "1.18.2"
addSbtPlugin("ch.epfl.scala"    % "sbt-scalafix"     % "0.14.3")
addSbtPlugin("org.scalameta"    % "sbt-mdoc"         % "2.6.1")

resolvers ++= Resolver.sonatypeOssRepos("public")
