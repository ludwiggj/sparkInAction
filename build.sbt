name := "sia"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0"
)

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
//  "org.apache.commons" % "commons-email" % "1.3.1" % "compile"
//)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}