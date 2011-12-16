name := "hazelcast-scala"

version := "0.1"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "javax.servlet" % "servlet-api" % "2.5",
  "com.hazelcast" % "hazelcast" % "1.9.4.4",
  "org.specs2" %% "specs2" % "1.6.1",
  "org.specs2" %% "specs2-scalaz-core" % "6.0.1" % "test",
  "org.scalaz" %% "scalaz-core" % "6.0.3",
  "com.google.guava" % "guava" % "10.0.1"
)	