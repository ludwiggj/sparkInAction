lazy val commonSettings = Seq(
  name := "sia",
  version := "0.1",
  scalaVersion := "2.11.8",
  resolvers ++= Seq(
    "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
  ),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

lazy val sparkSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.0",
    "org.apache.spark" %% "spark-sql" % "2.0.0"
  )
)

lazy val ch02 = (project in file("ch02")).
  settings(commonSettings: _*).
  settings(sparkSettings: _*)

lazy val ch03 = (project in file("ch03")).
  settings(commonSettings: _*).
  settings(sparkSettings: _*)

lazy val ch04 = (project in file("ch04")).
  settings(commonSettings: _*).
  settings(sparkSettings: _*)