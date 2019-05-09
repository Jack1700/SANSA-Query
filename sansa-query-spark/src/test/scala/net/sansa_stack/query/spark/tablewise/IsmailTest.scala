package net.sansa_stack.query.spark.tablewise

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
   // val query = getClass.getResource("/queries/bsbm/Q1.sparql").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    val df = triples.toDF()
    // query from test resources 
    val Q1 = """SELECT ?X
    WHERE {
        ?X <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature> <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature40> .
    }"""
    // our translation 
    val result = OurProgram.createQueryExecution(spark, Q1)
    // right query gives 7 back 
    // val result = spark.sql("SELECT X FROM (SELECT  triples.s  AS X , triples.p , triples.o  FROM triples  WHERE  triples.p=\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature\" And  triples.o= \"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature40\")")
    print(result.count())
    assert(result.count() == 7)
  }
  

}