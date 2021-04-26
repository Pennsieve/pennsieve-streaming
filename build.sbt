organization := "com.pennsieve"
name := "pennsieve-streaming"
version := "1.0"

scalaVersion := "2.12.11"

val AkkaHttpVersion = "10.1.11"
val AkkaVersion = "2.6.5"

organization := "com.pennsieve"
organizationName := "University of Pennsylvania"
licenses := List("Apache-2.0" -> new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))
startYear := Some(2021)
enablePlugins(AutomateHeaderPlugin)

resolvers ++= Seq(
  "pennsieve-maven-proxy" at "https://nexus.pennsieve.cc/repository/maven-public",
  Resolver.url("pennsieve-ivy-proxy", url("https://nexus.pennsieve.cc/repository/ivy-public/"))( Patterns("[organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]") ),
  "JBoss" at "https://repository.jboss.org/",
  "Spray" at "http://repo.spray.io",
  Resolver.bintrayRepo("commercetools", "maven"),
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases")
)

scalacOptions ++= Seq(
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-Ypartial-unification",
  "-Ywarn-unused",
  "-Xmax-classfile-name",
  "100",
  "-feature",
  "-deprecation"
)

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "nexus.pennsieve.cc",
  sys.env("PENNSIEVE_NEXUS_USER"),
  sys.env("PENNSIEVE_NEXUS_PW")
)

libraryDependencies ++= Seq(
  "com.pennsieve" %% "core-models" % "27-36566c3",
  "com.pennsieve" %% "pennsieve-core" % "27-36566c3",
  "com.pennsieve" %% "timeseries-core" % "4-d8f62a4",
  "com.pennsieve" %% "service-utilities" % "7-3a0e351",
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
  "de.knutwalker" %% "akka-stream-circe" % "3.4.0",
  "de.knutwalker" %% "akka-http-circe" % "3.4.0",
  "uk.me.berndporr" % "iirj" % "1.1",
  "net.debasishg" %% "redisclient" % "3.30",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "jul-to-slf4j" % "1.7.25",
  "org.slf4j" % "jcl-over-slf4j" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "ch.qos.logback" % "logback-core" % "1.2.3",
  "net.logstash.logback" % "logstash-logback-encoder" % "5.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.postgresql" % "postgresql" % "42.1.4",
  "org.scalikejdbc" %% "scalikejdbc" % "2.5.0",
  "org.scalikejdbc" %% "scalikejdbc-config" % "2.5.0",
  "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.6",
  "com.h2database" % "h2" % "1.4.193" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalikejdbc" %% "scalikejdbc-test" % "2.5.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion % Test
)

addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.17")

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("contribs", "mx", xs @ _*) => MergeStrategy.last
  case PathList("com", "sun", xs @ _*) => MergeStrategy.last
  case PathList("org.slf4j", xs @ _*) => MergeStrategy.last

  case "core-default.xml" => MergeStrategy.last
  case "common-version-info.properties" => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last

  case "about.html" => MergeStrategy.rename
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last

  case PathList("reference.conf") => MergeStrategy.concat

  case PathList("META-INf", "io.netty.versions.properties") => MergeStrategy.discard
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard

  case PathList("META-INF", xs @ _*) =>
    xs map { _.toLowerCase } match {
      case ps @ (x :: xs)
          if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa") =>
        MergeStrategy.discard
      case _ => MergeStrategy.last
    }
  case x => MergeStrategy.last
}

ThisBuild / scalafmtOnCompile := true

test in assembly := {}

enablePlugins(sbtdocker.DockerPlugin)

dockerfile in docker := {
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"
  new Dockerfile {
    from("pennsieve/java-cloudwrap:8-jre-alpine-0.5.9")
    copy(artifact, artifactTargetPath, chown="pennsieve:pennsieve")
    copy(baseDirectory.value / "bin" / "run.sh", "/app/run.sh", chown="pennsieve:pennsieve")
    run("mkdir", "-p", "/home/pennsieve/.postgresql")
    run("wget", "-qO", "/home/pennsieve/.postgresql/root.crt", "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem")
    cmd("--service", "pennsieve-streaming", "exec", "/app/run.sh", artifactTargetPath)
  }
}

imageNames in docker := Seq(
  ImageName(s"pennsieve/pennsieve-streaming:latest"),
  ImageName(
    s"pennsieve/pennsieve-streaming:${sys.props.getOrElse("docker-version", version.value)}"
  )
)
