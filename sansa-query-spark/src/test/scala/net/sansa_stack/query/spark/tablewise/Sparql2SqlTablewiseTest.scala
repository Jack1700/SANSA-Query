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
    val query = "src/test/resources/queries/bsbm/Q2.sparql"
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
    val query = "src/test/resources/queries/bsbm/Q6.sparql"
    val fileContents = Source.fromFile(query).getLines.mkString
    
    // our translation 
    val result = OurProgram.createQueryExecution(spark, fileContents)
    print(result.count())
    assert(result.count() == 0)
  }
}