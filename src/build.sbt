// Author: Nat Tuck

lazy val root = (project in file(".")).
  settings(
    name := "PageRank",
    version := "1.0",
    mainClass in Compile := Some("main.PageRank")
  )

// Scala Runtime
libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

//spark
libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
