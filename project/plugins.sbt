logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")

addCompilerPlugin( "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.1")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")
