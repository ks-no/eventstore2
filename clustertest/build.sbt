name := "clustertests"

version := "2.2-SNAPSHOT"

scalaVersion := "2.10.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "artifactory" at "http://jenkins.usrv.ubergenkom.no/artifactory/libs-snapshot-local"

resolvers += "Local Maven Repository" at "file:///"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "no.ks" % "eventstore2" % "2.2-SNAPSHOT"

libraryDependencies+= "ch.qos.logback" % "logback-classic" % "1.0.13"

libraryDependencies+= "org.springframework" % "spring-context" % "3.1.3.RELEASE"

libraryDependencies+= "org.springframework" % "spring-jdbc" % "3.1.3.RELEASE"

libraryDependencies+= "org.springframework" % "spring-test" % "3.1.3.RELEASE"