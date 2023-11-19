ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "dashboard"
  )

val sparkVersion = "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.22"
