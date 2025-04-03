ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.CodeCrafters"
ThisBuild / organizationName := "CodeCrafters"

assembly / assemblyJarName := "redis.jar"

lazy val root = (project in file("."))
  .settings(
    name := "codecrafter-redis",
    // List your dependencies here
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.2.19",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % Test
    )
  )
