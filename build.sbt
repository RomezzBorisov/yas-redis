name := "yas-redis"

version := "0.1"

scalaVersion := "2.9.1"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.6.4" % "provided"

libraryDependencies +=  "com.typesafe.akka" % "akka-actor" % "2.0.3"

libraryDependencies += "org.jboss.netty" % "netty" % "3.2.7.Final"

libraryDependencies += "org.specs2" %% "specs2" % "1.12.1" % "test"

libraryDependencies += "org.mockito" % "mockito-all" % "1.8.4" % "test"
