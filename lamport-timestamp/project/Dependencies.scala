import sbt._

object Dependencies {
  val akkaVersion = "2.6.17"

  val runtimeLibraries =
    Seq("com.typesafe.akka" %% "akka-actor-typed" % akkaVersion, "ch.qos.logback" % "logback-classic" % "1.2.8")

  val testLibraries = Seq(
    "com.novocode"       % "junit-interface"          % "0.11"      % Test,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
    "org.scalatest"     %% "scalatest"                % "3.2.9"     % Test
  )

  val all: Seq[ModuleID] = runtimeLibraries ++ testLibraries
}
