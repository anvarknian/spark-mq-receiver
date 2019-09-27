name := "spark-mq-receiver"

version := "0.0.1"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.10.4","2.11.12")

spName := "ibm/spark-mq-receiver"

val sparkVer = "1.6.1"

sparkVersion := sparkVer

sparkComponents ++= Seq("streaming")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

spIncludeMaven := false

spAppendScalaVersion := true


libraryDependencies ++= Seq(
  "javax.jms" % "jms-api" % "1.1-rev-1",
  "org.apache.activemq" % "activemq-core" % "5.7.0" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.ibm.mq" % "com.ibm.mq.allclient" % "9.1.1.0"
)


// Modify this to the location of your MQ Jar files
unmanagedBase := baseDirectory.value / ".." / "MQJars"

/* An alternative method of provided the location of the jars you wish to include jars from multiple directories
unmanagedJars in Compile ++= {
    val base = baseDirectory.value
    val baseDirectories = (base / "MQJars")
    val customJars = (baseDirectories ** "*.jar")
    customJars.classpath
}
*/

assemblyJarName := "ibm-dependency.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}