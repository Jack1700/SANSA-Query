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

class Sparql2SqlTablewiseTest extends FunSuite with DataFrameSuiteBase {

  test("test main Test") {

    // This Test executes the Sparql query and the translated SQL version and compares the results
    val OurProgram = new Interface()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    var triples = spark.rdf(Lang.NTRIPLES)(input)
     val df = triples.toDF()

    // Extracts the query from the test resources
    val queryPath = "src/test/resources/queries/bsbm/Q9.sparql"
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
  
  
//    test("test Filter Problem") {
//
//    // This Test executes the Sparql query and the translated SQL version and compares the results
//    val OurProgram = new Interface()
//    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
//    var triples = spark.rdf(Lang.NTRIPLES)(input)
//
//   // val newTriples = triples.map(t => {(t.getSubject().toString().split("\\^^")(0),t.getPredicate().toString().split("\\^^")(0),t.getObject().toString().split("\\^")(0)) })
//    
//    val newTriples = triples.map(t => {(t.getSubject().getLiteral().getLexicalForm,t.getPredicate().getLiteral().getLexicalForm,t.getObject().getLiteral().getLexicalForm) })
//    val df = triples.toDF()
//
//    // Extracts the query from the test resources
//    val queryPath = "src/test/resources/queries/bsbm/Q8.sparql"
//    val fileContents = Source.fromFile(queryPath).getLines.mkString
//
//    // Executes the Sparql query (result resultset in r)
//    val defModel = ModelFactory.createDefaultModel();
//    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
//    val sparqlQuery = QueryFactory.create(fileContents);
//    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
//    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
//    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
//    val r = qexec.execSelect();
//
//    // Converts the resultset to a CSV file
//    val sparqlResultFile = new File("src/test/resources/testresults/results.csv")
//    val outputSteam = new FileOutputStream(sparqlResultFile)
//    ResultSetFormatter.outputAsCSV(outputSteam, r)
//
//    // Converts the saved CSV file into a dataframe
//    val sparqlDataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/testresults/results.csv")
//
//    // Executes the Translator
//    val result = OurProgram.createQueryExecution(spark, fileContents)
//    println(OurProgram.Sparql2SqlTablewise(fileContents))
//
//    val TestQuery = spark.sql("""SELECT Q0.product , Q0.value1, CAST(Q0.value1 AS INT) FROM 
//  (SELECT  triples.s AS product  ,  triples.o AS value1  FROM triples  WHERE  triples.p="http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1") Q0 """)
//
//    TestQuery.collect.foreach(println)
//    // Compares both results
//    println("Sparql: " + r.getRowNumber())
//    val intersection = result.intersect(sparqlDataFrame)
//    assert(result.count() == sparqlDataFrame.count() && result.count() == intersection.count())
//  }
}