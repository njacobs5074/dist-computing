ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

inThisBuild(
  List(
    scalaVersion               := "2.13.7",
    scalafixScalaBinaryVersion := "2.13",
    semanticdbEnabled          := true,
    semanticdbVersion          := scalafixSemanticdb.revision
  )
)

lazy val root = project
  .in(file("."))
  .settings(
    name    := "lamport-timestamp",
    version := "0.1.0-SNAPSHOT",
    scalacOptions += "-Wunused",
    libraryDependencies ++= Dependencies.all
  )
