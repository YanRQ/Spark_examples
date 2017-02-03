name := "rec1"

version := "1.0"

scalaVersion := "2.11.7"

//libraryDependencies  += "org.apache.spark" % "spark-core_2.11" % "1.3.0"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "1.3.0",
  "org.apache.spark" % "spark-mllib_2.11" % "1.3.0",
  "org.apache.spark" % "spark-streaming_2.11" % "1.3.0",
  "org.apache.commons" % "commons-math3" % "3.3",
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test")

resolvers  ++= Seq("Apache Repository" at "https://repository.apache.org/content/repositories/releases",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")
