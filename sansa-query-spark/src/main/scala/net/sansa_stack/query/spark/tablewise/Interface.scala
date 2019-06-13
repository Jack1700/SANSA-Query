package net.sansa_stack.query.spark.tablewise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }


class Interface {
  val t = new HelperFunctions 
  /*
  Translates and executes a Sparql query tablewise in SQL and returns the dataframe
  
  Given: Spark session; Sparql query string
  */
  def createQueryExecution(spark: SparkSession, queryString: String) : DataFrame = {
    
    val sqlQuery = Sparql2SqlTablewise(queryString)
    val dataframe = spark.sql(sqlQuery)
    
    println(t.getDataType(dataframe, "x"))
    
    
    
    return dataframe
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