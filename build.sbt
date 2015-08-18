import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

seq(assemblySettings: _*)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
    case x => MergeStrategy.first
}
}

name := "ModelMerge"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.4.0"

libraryDependencies += "io.spray" %%  "spray-json" % "1.2.6"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
