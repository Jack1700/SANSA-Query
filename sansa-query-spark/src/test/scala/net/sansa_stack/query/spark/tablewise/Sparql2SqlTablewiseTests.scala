package net.sansa_stack.query.spark.tablewise

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import net.sansa_stack.query.spark.query._
import 


class Sparql2SqlTablewiseTests extends FunSuite with DataFrameSuiteBase {


  val configFile = getClass.getResource("/config").getPath
  val mappingsFile = getClass.getResource("/mappings.ttl").getPath

  test("running BSBM Q1 should result 10") {

    val query = getClass.getResource("/queries/Q1.sparql").getPath

    val Sparql2Sql = new Sparql2SqlTablewise()
    val sqlQuery = Sparql2Sql.Sparql2SqlTablewise("SELECT ?X WHERE { ?X <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature> <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature40> .}");

    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 10)
  }

  test("running BSBM Q2 should result 200") {

    val query = getClass.getResource("/queries/Q2.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 200)
  }

  test("running BSBM Q3 should result 0") {

    val query = getClass.getResource("/queries/Q3.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 0)
  }

  test("running BSBM Q4 should result 7") {

    val query = getClass.getResource("/queries/Q4.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 7)
  }

  test("running BSBM Q5 should result 0") {

    val query = getClass.getResource("/queries/Q5.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 0)
  }

  test("running BSBM Q6 should result 0") {

    val query = getClass.getResource("/queries/Q6.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 0)
  }

  
  
  
  
  def compareCSV(SparqlCSV: String, SqlCSV: String): int = {
    
    
    
    val SparqlArray = new ArrayBuffer()
    val bufferedSource = io.Source.fromFile(SparqlCSV) //"/tmp/finance.csv"
    
    for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        SparqlArray += cols
    }
    bufferedSource.close
    
    val SqlArray = new ArrayBuffer()
    val bufferedSource = io.Source.fromFile(SqlCSV)
    
    for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        SqlArray += cols
    }
    bufferedSource.close
    
    if(SparqlArray.size != SqlArray.size) {
      return 1
    } else {
      for(int i = 0; i < SparqlArray.size; i++) {
        
        val found = 0
        
        for(int j = 0; j < SqlArray.size; j++) {
          if(SparqlArray[i] == Sql[j]) {
            found = 1
            SparqlArray -= SparqlArray[i]
            SqlArray -= SqlArray[j]
            break
          }
        }
        
        if(found == 0) {
          return 1
        }
      }
    }
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}