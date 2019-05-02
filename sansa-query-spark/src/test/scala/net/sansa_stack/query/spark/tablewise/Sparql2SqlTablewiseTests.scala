package net.sansa_stack.query.spark.tablewise

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
//import net.sansa_stack.query.spark.tablewise.Sparql2SqlTablewise
import net.sansa_stack.query.spark.query._



class Sparql2SqlTablewiseTests extends FunSuite with DataFrameSuiteBase {


  val configFile = getClass.getResource("/config").getPath
  val mappingsFile = getClass.getResource("/mappings.ttl").getPath

  test("running BSBM Q1 should result 10") {

    val query = getClass.getResource("/queries/Q1.sparql").getPath
    val Sparql2Sql = new Sparql2SqlTablewise()
    val sqlQuery = Sparql2Sql.Sparql2SqlTablewise(query);
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

}