
val scalaBuidVersion = "2.11"
val sparkVersion = "2.0.0"
val elasticsearchSparkDriverVersion = "5.0.0-alpha5"
val elasticsearchSparkConnector = "org.elasticsearch" % s"elasticsearch-spark-20_${scalaBuidVersion}" % elasticsearchSparkDriverVersion
val typeSafeVersion = "1.2.1"
val ficusVersion = "1.1.2"
val typesafeConfig  = "com.typesafe" % "config" % typeSafeVersion
val ficus = "net.ceedubs" % s"ficus_${scalaBuidVersion}" % ficusVersion

lazy val commonSettings = Seq(
  organization := "com.ftel",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.7"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "http_sender",
    libraryDependencies ++= Seq(
      "org.apache.spark" % s"spark-core_${scalaBuidVersion}" % sparkVersion % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
      "org.apache.spark" % s"spark-sql_${scalaBuidVersion}" % sparkVersion,
      "org.apache.spark" % s"spark-streaming_${scalaBuidVersion}" % sparkVersion,
      "com.databricks" % s"spark-xml_${scalaBuidVersion}" % "0.4.1",
      //"org.elasticsearch" % s"elasticsearch-spark-20_${scalaBuidVersion}" % elasticsearchSparkDriverVersion,
      elasticsearchSparkConnector,
      typesafeConfig,
      ficus
    )
  )
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "scala-library.jar"}
  cp filter {_.data.getName == "junit-3.8.1.jar"}
}

resolvers += Resolver.mavenLocal