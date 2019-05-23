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

class Sparql2SqlTablewiseTest extends FunSuite with DataFrameSuiteBase {


  test("test 4") {
    // THIS TEST EXCUTES THE ACTUAL SPARQL QUERY THEN COMPARE THE RESULTS WITH SQL QUERY. WORKS PERFECTLY
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // query from test resources
    val queryPath = "src/test/resources/queries/bsbm/Q6.sparql"
    val fileContents = Source.fromFile(queryPath).getLines.mkString

    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();
    val sparqlResultFile = new File("src/test/resources/testresults/results.csv")
    val outputSteam = new FileOutputStream(sparqlResultFile);
    ResultSetFormatter.outputAsCSV(outputSteam, r); //prints out the result
    println("this is number of rows with sparql " + r.getRowNumber())

    val sparqlDataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/testresults/results.csv")

    val result = OurProgram.createQueryExecution(spark, fileContents)
 
    val intersection = result.intersect(sparqlDataFrame)
    assert(result.count() == sparqlDataFrame.count() && result.count() == intersection.count() )
  }
}