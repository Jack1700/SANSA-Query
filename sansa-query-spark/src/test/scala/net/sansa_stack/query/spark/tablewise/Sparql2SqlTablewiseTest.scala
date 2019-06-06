package net.sansa_stack.query.spark.tablewise

import scala.io.Source
import java.io.File
import java.io.FileOutputStream
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.query.Syntax
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.shared.PrefixMapping
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.Node
import org.apache.commons.rdf.api.Triple
import org.apache.spark.sql.SQLContext
import scala.xml.factory.NodeFactory

class Sparql2SqlTablewiseTest extends FunSuite with DataFrameSuiteBase {

  test("test MAIN") {

    // This Test executes the Sparql query and the translated SQL version and compares the results
    val OurProgram = new Interface()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    var triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // Extracts the query from the test resources
    val queryPath = "src/test/resources/queries/bsbm/Q_MAIN.sparql"
    val fileContents = Source.fromFile(queryPath).getLines.mkString

    // Executes the Sparql query (result resultset in r)
    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();

    // Converts the resultset to a CSV file
    val sparqlResultFile = new File("src/test/resources/testresults/results.csv")
    val outputSteam = new FileOutputStream(sparqlResultFile)
    ResultSetFormatter.outputAsCSV(outputSteam, r)

    // Converts the saved CSV file into a dataframe
    val sparqlDataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/testresults/results.csv")

    // Executes the Translator
    val result = OurProgram.createQueryExecution(spark, fileContents)
    println(OurProgram.Sparql2SqlTablewise(fileContents))

    // Compares both results
    println("Sparql: " + r.getRowNumber())
    val intersection = result.intersect(sparqlDataFrame)
    assert(result.count() == sparqlDataFrame.count() && result.count() == intersection.count())
  }

  test("test OPTIONAL") {

    // This Test executes the Sparql query and the translated SQL version and compares the results
    val OurProgram = new Interface()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    var triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // Extracts the query from the test resources
    val queryPath = "src/test/resources/queries/bsbm/Q_OPTIONAL.sparql"
    val fileContents = Source.fromFile(queryPath).getLines.mkString

    // Executes the Sparql query (result resultset in r)
    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();

    // Converts the resultset to a CSV file
    val sparqlResultFile = new File("src/test/resources/testresults/results.csv")
    val outputSteam = new FileOutputStream(sparqlResultFile)
    ResultSetFormatter.outputAsCSV(outputSteam, r)

    // Converts the saved CSV file into a dataframe
    val sparqlDataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/testresults/results.csv")

    // Executes the Translator
    val result = OurProgram.createQueryExecution(spark, fileContents)
    println(OurProgram.Sparql2SqlTablewise(fileContents))

    // Compares both results
    println("Sparql: " + r.getRowNumber())
    val intersection = result.intersect(sparqlDataFrame)
    assert(result.count() == sparqlDataFrame.count() && result.count() == intersection.count())
  }

  test("test DISTINCT") {

    val OurProgram = new Interface()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    var triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // Extracts the query from the test resources
    val queryPath = "src/test/resources/queries/bsbm/Q_DISTINCT.sparql"
    val fileContents = Source.fromFile(queryPath).getLines.mkString

    // Executes the Sparql query (resultset in r)
    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();

    // Converts the resultset to a CSV file
    val sparqlResultFile = new File("src/test/resources/testresults/results.csv")
    val outputSteam = new FileOutputStream(sparqlResultFile)
    ResultSetFormatter.outputAsCSV(outputSteam, r)

    // Converts the saved CSV file into a dataframe
    val sparqlDataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/testresults/results.csv")

    // Executes the Translator
    val result = OurProgram.createQueryExecution(spark, fileContents)
    println(OurProgram.Sparql2SqlTablewise(fileContents))

    // Compares both results
    println("Sparql: " + r.getRowNumber())
    val intersection = result.intersect(sparqlDataFrame)
    assert(result.count() == sparqlDataFrame.count() && result.count() == intersection.count())
  }

  test("test FILTER") {

    // This Test executes the Sparql query and the translated SQL version and compares the results
    val OurProgram = new Interface()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    var triples = spark.rdf(Lang.NTRIPLES)(input)
 
     triples = triples.map(t => { var  s=""
                                        var p =""
                                        var o =""
                                        if (t.getSubject().isLiteral())  s = t.getSubject().getLiteral().getLexicalForm else s=t.getSubject().toString()
                                        if (t.getPredicate().isLiteral())  p = t.getPredicate().getLiteral().getLexicalForm else p=t.getPredicate().toString()
                                        if (t.getObject().isLiteral())  o = t.getObject().getLiteral().getLexicalForm else o=t.getObject().toString()
      new org.apache.jena.graph.Triple(org.apache.jena.graph.NodeFactory.createBlankNode(s),
                                        org.apache.jena.graph.NodeFactory.createBlankNode(p),
                                        org.apache.jena.graph.NodeFactory.createBlankNode(o)) })
 
    val df = triples.toDF()
    // Extracts the query from the test resources
    val queryPath = "src/test/resources/queries/bsbm/Q_FILTER3.sparql"
    val fileContents = Source.fromFile(queryPath).getLines.mkString

    // Executes the Sparql query (result resultset in r)
    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();

    // Converts the resultset to a CSV file
    val sparqlResultFile = new File("src/test/resources/testresults/results.csv")
    val outputSteam = new FileOutputStream(sparqlResultFile)
    ResultSetFormatter.outputAsCSV(outputSteam, r)

    // Converts the saved CSV file into a dataframe
    val sparqlDataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/testresults/results.csv")

    // Executes the Translator
    val result = OurProgram.createQueryExecution(spark, fileContents)
    println(OurProgram.Sparql2SqlTablewise(fileContents))

    println("Sparql: " + r.getRowNumber())
    val intersection = result.intersect(sparqlDataFrame)
    assert(result.count() == sparqlDataFrame.count() && result.count() == intersection.count())
  }

}