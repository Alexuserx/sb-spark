ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "lab03"
  )

val sparkVersion = "2.4.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.2"
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.3"