val zioSbtVersion = "0.8.7"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-ci"        % zioSbtVersion)

addSbtPlugin("org.scoverage"    % "sbt-scoverage"    % "2.4.4")
addSbtPlugin("com.typesafe"     % "sbt-mima-plugin"  % "1.1.6")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"    % "0.13.2")
addSbtPlugin("org.scala-native" % "sbt-scala-native" % "0.5.10")
addSbtPlugin("org.scala-js"     % "sbt-scalajs"      % "1.22.0")
