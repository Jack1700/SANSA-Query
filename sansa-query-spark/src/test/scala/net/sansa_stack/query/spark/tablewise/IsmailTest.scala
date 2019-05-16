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

class IsmailTest extends FunSuite with DataFrameSuiteBase {
  
  test("test 1") {
    //create Spark Session
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    //val query = getClass.getResource("/queries/bsbm/Q1.sparql").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()
    
    // query from test resources 
    val query = "src/test/resources/queries/bsbm/Q1.sparql"
    val fileContents = Source.fromFile(query).getLines.mkString
    
    // our translation 
    val result = OurProgram.createQueryExecution(spark, fileContents)
    
    // correct query should return 7 matches 
    print(result.count())
    assert(result.count() == 7)
  }
  
   test("test 2") {
    //create Spark Session
    val OurProgram = new Sparql2SqlTablewise()
    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
   // val query = getClass.getResource("/queries/bsbm/Q1.sparql").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()
    
    // query from test resources 
     val query = "src/test/resources/queries/bsbm/Q2.sparql"
    val fileContents = Source.fromFile(query).getLines.mkString
    print(fileContents)
    
    // our translation 
    val result = OurProgram.createQueryExecution(spark, fileContents)
    
    // right query gives 4 back 
    print(result.count())
    assert(result.count() == 4)
  }
  

}