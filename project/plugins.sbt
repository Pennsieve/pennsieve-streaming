logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5" exclude("org.apache.maven", "maven.plugin-api"))

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.1")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")

addCompilerPlugin( "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.1")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.6.0-RC4")
