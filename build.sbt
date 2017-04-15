import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

name := "universal-recommender"

name := "template-java-parallel-universal-recommendation"

version := "0.1.0"

organization := "com.actionml"

val mahoutVersion = "0.13.0-SNAPSHOT"

val pioVersion = "0.10.0-incubating"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % pioVersion % "provided",
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.4.0" % "provided",
  "org.xerial.snappy" % "snappy-java" % "1.1.1.7",
  // Mahout's Spark libs
  "org.apache.mahout" %% "mahout-math-scala" % mahoutVersion,
  "org.apache.mahout" %% "mahout-spark" % mahoutVersion
    exclude("org.apache.spark", "spark-core_2.10"),
  "org.apache.mahout"  % "mahout-math" % mahoutVersion,
  "org.apache.mahout"  % "mahout-hdfs" % mahoutVersion
    exclude("com.thoughtworks.xstream", "xstream")
    exclude("org.apache.hadoop", "hadoop-client"),
  // other libs
  "com.thoughtworks.xstream" % "xstream" % "1.4.4"
    exclude("xmlpull", "xmlpull"),
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.2"
    exclude("org.apache.spark", "spark-catalyst_2.10")
    exclude("org.apache.spark", "spark-sql_2.10"),
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.projectlombok" % "lombok" % "1.16.14",
  "com.google.code.gson"    % "gson"             % "2.5",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "com.fatboyindustrial.gson-jodatime-serialisers" % "gson-jodatime-serialisers" % "1.6.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "com.google.guava" % "guava" % "12.0",
  "org.jblas" % "jblas" % "1.2.4")
  .map(_.exclude("org.apache.lucene","lucene-core")).map(_.exclude("org.apache.lucene","lucene-analyzers-common"))

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(DanglingCloseParenthesis, Prevent)
  .setPreference(MultilineScaladocCommentsStartOnFirstLine, true)

assemblyMergeStrategy in assembly := {
  case "plugin.properties" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "package-info.class" =>
    MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")

parallelExecution in test := false