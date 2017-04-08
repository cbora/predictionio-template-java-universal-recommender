

assemblySettings

name := "barebone-template"

organization := "io.prediction"

resolvers += Resolver.mavenLocal

val mahoutVersion = "0.13.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.10.0-incubating" % "provided",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided",
  // Mahout's Spark libs
  "org.apache.mahout" %% "mahout-math-scala" % mahoutVersion,
  "org.apache.mahout" %% "mahout-spark" % mahoutVersion
    exclude("org.apache.spark", "spark-core_2.10"),
  "org.apache.mahout"  % "mahout-math" % mahoutVersion,
  "org.apache.mahout"  % "mahout-hdfs" % mahoutVersion
    exclude("com.thoughtworks.xstream", "xstream")
    exclude("org.apache.hadoop", "hadoop-client"),
  // Elasticsearch integration
  "org.elasticsearch" %% "elasticsearch-spark" % "2.1.2",
  // junit testing
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "com.fatboyindustrial.gson-jodatime-serialisers" % "gson-jodatime-serialisers" % "1.6.0",
  // lombok
  "org.projectlombok" % "lombok" % "1.16.14",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "com.google.code.gson"    % "gson"             % "2.5",
  "com.google.guava" % "guava" % "12.0",
  "org.jblas" % "jblas" % "1.2.4"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")

parallelExecution in test := false