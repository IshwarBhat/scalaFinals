name := "finals"

version := "1.0"

scalaVersion := "2.11.8"


resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases"


resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"



libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"


libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"

libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6"


libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.1.0"

libraryDependencies += "graphframes" % "graphframes" % "0.4.0-spark2.1-s_2.11"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.17"

libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.13"

libraryDependencies += "org.scalanlp" % "breeze-viz_2.11" % "0.13"


fork in run := true