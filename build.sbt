name := "AkkaPlayground"

version := "0.0.1"

scalaVersion := "2.12.4"

resolvers += Resolver.bintrayRepo("ovotech", "maven")
resolvers += "confluent" at "http://packages.confluent.io/maven/"


libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "0.18" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % "test"
libraryDependencies += "com.ovoenergy" %% "kafka-serialization-avro4s" % "0.3.8"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "4.0.0"
libraryDependencies += "io.confluent" % "kafka-schema-registry" % "4.0.0"