name := "clustertests"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.0-RC5"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "artifactory" at "http://jenkins.usrv.ubergenkom.no/artifactory/libs-snapshot-local"

resolvers += "Local Maven Repository" at "file:///"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "no.ks" % "eventstore2" % "1.0-SNAPSHOT"

libraryDependencies+= "ch.qos.logback" % "logback-classic" % "1.0.7"

