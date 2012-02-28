
name := "smr"

version := "0.2-SNAPSHOT"

organization := "org.scalanlp"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1")

resolvers ++= Seq(
  "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0-RC1"

libraryDependencies += "com.typesafe.akka" % "akka-remote" % "2.0-RC1"

libraryDependencies += "junit" % "junit" % "4.5" % "test"

libraryDependencies <<= (scalaVersion, libraryDependencies) { (sv, deps) =>
  sv match {
    case "2.9.1" =>
      (deps :+ ("org.scalatest" % "scalatest" % "1.4.RC2" % "test")
            :+ ("org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"))
    case x if x.startsWith("2.8") =>
      (deps :+ ("org.scalatest" % "scalatest" % "1.3" % "test")
            :+ ("org.scala-tools.testing" % "scalacheck_2.8.1" % "1.8" % "test"))
    case x  => error("Unsupported Scala version " + x)
  }
}


