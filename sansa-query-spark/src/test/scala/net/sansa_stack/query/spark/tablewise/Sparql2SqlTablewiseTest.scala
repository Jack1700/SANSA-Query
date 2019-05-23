package net.sansa_stack.query.spark.tablewise
import scala.io.Source

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import net.sansa_stack.query.spark.tablewise
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.stats._
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.model._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory, ResultSetFormatter, Syntax }
import org.apache.jena.tdb.TDBFactory
import org.apache.jena.query.ResultSet
import org.apache.jena.query.QueryExecution
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.rdf.model.ModelFactory

class Sparql2SqlTablewiseTest extends FunSuite with DataFrameSuiteBase {

  test("test 1") {
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // query from test resources
    val query = "src/test/resources/queries/bsbm/Q1.sparql"
    val fileContents = Source.fromFile(query).getLines.mkString

    // our translation
    val result = OurProgram.createQueryExecution(spark, fileContents)
    print(result.count())
    assert(result.count() == 7)
  }

  test("test 2") {
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // query from test resources
    val query = "src/test/resources/queries/bsbm/Q5.sparql"
    val fileContents = Source.fromFile(query).getLines.mkString

    
    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();
    ResultSetFormatter.outputAsCSV(r); //prints out the result
    println("this is number of rows with sparql " + r.getRowNumber() + "\n")
    
    
    // our translation
    val result = OurProgram.createQueryExecution(spark, fileContents)
    print(result.count())
    assert(result.count() == 674)
  }

  test("test 3") {
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // query from test resources
    val query = "src/test/resources/queries/bsbm/Q6.sparql"
    val fileContents = Source.fromFile(query).getLines.mkString
    
    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();
    
    // our translation
    val result = OurProgram.createQueryExecution(spark, fileContents)
    print(result.count())
    assert(result.count() == r.getRowNumber)
  }

  test("test 4") {
    // THIS TEST EXCUTES THE ACTUAL SPARQL QUERY THEN COMPARE THE RESULTS WITH SQL QUERY. WORKS PERFECTLY
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()

    // query from test resources
    val queryPath = "src/test/resources/queries/bsbm/Q8.sparql"
    val fileContents = Source.fromFile(queryPath).getLines.mkString

    val defModel = ModelFactory.createDefaultModel();
    val mymodel = defModel.read("src/test/resources/datasets/bsbm-sample.nt");
    val sparqlQuery = QueryFactory.create(fileContents);
    sparqlQuery.setPrefixMapping(PrefixMapping.Standard);
    QueryFactory.parse(sparqlQuery, fileContents, "", Syntax.syntaxSPARQL_11);
    val qexec = QueryExecutionFactory.create(sparqlQuery, mymodel);
    val r = qexec.execSelect();
    ResultSetFormatter.outputAsCSV(r); //prints out the result
    println("this is number of rows with sparql " + r.getRowNumber + "\n")

    // our translation

    val result = OurProgram.createQueryExecution(spark, fileContents)
    print(result.count())
    assert(result.count() == r.getRowNumber)
//    assert(result.count() == 98)
  
  }
}