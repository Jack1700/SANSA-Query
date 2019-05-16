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
    //val query = "src/test/resources/queries/bsbm/Q5.sparql"
    //val fileContents = Source.fromFile(query).getLines.mkString
    // our translation 
    val result = OurProgram.createQueryExecution(spark, """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/spec/>
PREFIX schema: <http://schema.org/>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX edm: <http://www.europeana.eu/schemas/edm/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX gr: <http://purl.org/goodrelations/v1#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
SELECT ?productLabel ?price ?vendor ?revTitle ?reviewer ?rating1 ?rating2 ?product ?revName
WHERE {
    ?product rdfs:label ?productLabel .
    ?product rdf:type bsbm:Product .
    ?offer bsbm:product ?product .
    ?offer bsbm:price ?price .
    ?offer bsbm:vendor ?vendor .
    ?offer bsbm:validTo ?date .
    ?review bsbm:reviewFor ?product .
    ?review rev:reviewer ?reviewer .
    ?review dc:title ?revTitle .
    ?review bsbm:rating1 ?rating1 .
    ?review bsbm:rat ing2 ?rating2 .
    ?reviewer foaf:name ?revName .
    ?reviewer a foaf:Person .
    FILTER(?product = 9)
    FILTER(?price > 5000)
}""")
    print(result.count())
    assert(result.count() == 0)
  }
}