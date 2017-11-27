name := "AkkaPlayground"

version := "0.0.1"

scalaVersion := "2.12.4"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "0.18" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "0.16.0" % "test"