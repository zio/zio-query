addSbtPlugin("org.scala-js"                      % "sbt-scalajs"               % "1.11.0")
addSbtPlugin("org.portable-scala"                % "sbt-scalajs-crossproject"  % "1.2.0")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"              % "2.4.6")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"                   % "0.4.3")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"             % "0.11.0")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"             % "2.0.6")
addSbtPlugin("ch.epfl.scala"                     % "sbt-bloop"                 % "1.5.4")
addSbtPlugin("com.github.sbt"                    % "sbt-unidoc"                % "0.5.0")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"            % "1.5.10")
addSbtPlugin("com.github.cb372"                  % "sbt-explicit-dependencies" % "0.2.16")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"          % "3.0.2")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"              % "0.10.4")
addSbtPlugin("dev.zio"                           % "zio-sbt-website"           % "0.0.0+84-6fd7d64e-SNAPSHOT")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.5"

resolvers += Resolver.sonatypeRepo("public")
