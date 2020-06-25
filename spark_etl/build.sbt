name := "spark_etl"

version := "0.1"

scalaVersion := "2.12.11"

val hadoopVersion = "2.8.5"
val sparkVersion = "2.4.5"

resolvers += "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

// Note the dependencies are provided
libraryDependencies ++= Vector(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7",
  "com.fasterxml.jackson.core"   % "jackson-databind"      % "2.8.7",
  "com.fasterxml.jackson.core"   % "jackson-core"          % "2.8.7",
  "org.apache.spark"             %% "spark-core"           % sparkVersion,
  "org.apache.spark"             %% "spark-sql"            % sparkVersion,
  "com.amazonaws"                % "aws-java-sdk-s3"       % "1.11.804",
  "com.github.javafaker"         % "javafaker"             % "1.0.2",
  "nl.basjes.parse.useragent"    % "yauaa"                 % "5.18",
  "com.maxmind.geoip2"           % "geoip2"                % "2.14.0" excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core")),
  "com.amazon.redshift"          % "redshift-jdbc42"       % "1.2.1.1001",
  "org.apache.hadoop"            % "hadoop-aws"            % hadoopVersion,
  "org.apache.hadoop"            % "hadoop-hdfs"           % hadoopVersion,
  "org.apache.hadoop"            % "hadoop-client"         % hadoopVersion,
  "org.scalatest"                %% "scalatest"            % "3.1.2" % Test,
)

addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.10")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
  case x                                         => MergeStrategy.first
}
