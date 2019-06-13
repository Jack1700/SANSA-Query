package net.sansa_stack.query.spark.tablewise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }
import scala.collection.mutable.ArrayBuffer



class Interface {
  val t = new HelperFunctions 
  /*
  Translates and executes a Sparql query tablewise in SQL and returns the dataframe
  
  Given: Spark session; Sparql query string
  */
  def createQueryExecution(spark: SparkSession, queryString: String) : DataFrame = {
    
    val sqlQuery = Sparql2SqlTablewise(queryString)
    val dataframe = spark.sql(sqlQuery)
    
    var types = new ArrayBuffer[String]
    for(column <- SparqlAnalyzer.orderByVariables) {
      types += t.getDataType(dataframe, column)
    }
    
    val dataframe2 = t.mapToTypedDF(dataframe, SparqlAnalyzer.orderByVariables, types)    
    
    dataframe2.collect.foreach(println)
    
    return dataframe2
  }
  
  
  /*
  Translates a Sparql query tablewise into SQL
  
  Given: Sparql query string
  */
  def Sparql2SqlTablewise(queryString: String) : String = {

    val translator = new Sparql2SqlTablewise

    return translator.assembleQuery(queryString)
  }
  
  
}