
name := "home-task"
version := "0.1"
scalaVersion := "2.12.4"


val sparkVersion = "2.4.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.rogach" %% "scallop" % "3.2.0"
)


