# SANSA Query
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-query-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-query-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA-Query/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA-Query/job/develop/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

## Description
SANSA Query is a library to perform queries directly into [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It allows files to reside in HDFS as well as in a local file system and distributes executions across Spark RDDs/DataFrames or Flink DataSets.

SANSA uses vertical partitioning (VP) approach and is designed to support extensible partitioning of RDF data. Instead of dealing with a single three-column table (s, p, o), data is partitioned into multiple tables based on the used RDF predicates, RDF term types and literal datatypes. The first column of these tables is always a string representing the subject. The second column always represents the literal value as a Scala/Java datatype. Tables for storing literals with language tags have an additional third string column for the language tag. Its uses [Sparqlify](https://github.com/AKSW/Sparqlify) as a scalable SPARQL-SQL rewriter.

### SANSA Query Spark
On SANSA Query Spark the method for partitioning an `RDD[Triple]` is located in [RdfPartitionUtilsSpark](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-spark/src/main/scala/net/sansa_stack/rdf/spark/partition/core/RdfPartitionUtilsSpark.scala). It uses an [RdfPartitioner](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartitioner.scala) which maps a Triple to a single [RdfPartition](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartition.scala) instance.

* [RdfPartition](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartition.scala) - as the name suggests, represents a partition of the RDF data and defines two methods:
  * `matches(Triple): Boolean`: This method is used to test whether a triple fits into a partition.
  * `layout: TripleLayout`: This method returns the [TripleLayout](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/layout/TripleLayout.scala) associated with the partition, as explained below.
  * Furthermore, RdfPartitions are expected to be serializable, and to define equals and hash code.
* TripleLayout instances are used to obtain framework-agnostic compact tabular representations of triples according to a partition. For this purpose it defines the two methods:
  * `fromTriple(triple: Triple): Product`: This method must, for a given triple, return its representation as a [Product](https://www.scala-lang.org/files/archive/api/2.11.8/index.html#scala.Product) (this is the super class of all Scala tuples)
  * `schema: Type`: This method must return the exact Scala type of the objects returned by `fromTriple`, such as `typeOf[Tuple2[String,Double]]`. Hence, layouts are expected to only yield instances of one specific type.

See the [available layouts](https://github.com/SANSA-Stack/SANSA-RDF/tree/develop/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/layout) for details.

## Usage

The following Scala code shows how to query an RDF file SPARQL syntax (be it a local file or a file residing in HDFS):
```scala

val spark: SparkSession = ...

val lang = Lang.NTRIPLES
val triples = spark.rdf(lang)("path/to/rdf.nt")


val partitions = RdfPartitionUtilsSpark.partitionGraph(triples)
val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)

val qef = new QueryExecutionFactorySparqlifySpark(spark, rewriter)

val port = 7531
val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()
server.join()

```
An overview is given in the [FAQ section of the SANSA project page](http://sansa-stack.net/faq/#sparql-queries). Further documentation about the builder objects can also be found on the [ScalaDoc page](http://sansa-stack.net/scaladocs/).

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.
