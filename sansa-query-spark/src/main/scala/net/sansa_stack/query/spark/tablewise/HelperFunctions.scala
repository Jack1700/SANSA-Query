package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.Queue
import org.apache.jena.sparql.core.Var
import scala.collection.mutable.ArrayBuffer
import java.util.List
import org.apache.spark.sql.DataFrame


class HelperFunctions {
  
  
  /*
  Converts a project variable like ?x into a for SQL usable format like Q0.x where Q0 is the corresponding BGP
  
  Given: List of project variables; List of BGP's (as SubQueries)
  */
  def cleanProjectVariables(projectVariables: List[Var], SubQuerys: Queue[SubQuery]): String = {
    
    var variables = new ArrayBuffer[String]
    var v = 0
    
    for (v <- 0 until projectVariables.size) {
      
      var variable = projectVariables.get(v).toString
      
      variable = variable.substring(1,variable.size)
      variables += getBgpWithVariable(variable, SubQuerys) + "." + variable
    }
    
    return variables.toString.substring(12, variables.toString.size - 1)
  }  
  
  
  /*
  Finds the number of the BGP where the variable first appears
  
  Given: a Variable as a String; List of BGP's (as SubQueries)
  */
  def getBgpWithVariable (variable: String, SubQuerys: Queue[SubQuery]) : String = {
  
    var i = 0
    
    for (i <- 0 until SubQuerys.size) {
      if (SubQuerys(i).variables.contains(variable)) {
        return "Q" + i
      }
    }
    return "FunctionFailed"
  }
  
  
  /*
  Finds a variable in an Array of variables (like Q0.x) and returns it's prefix (like Q0)
    
  Given: Variable; Array of variables
  */
  def getTableWithVariable(variable: String, variables: ArrayBuffer[String]) : String = {
    
    for (v <- variables) {
      val str = v.split("\\.")
      
      if (variable == str(1)) {
        return str(0)  
      }
    }
    
    return null
  }
  
  
  /*
  Determines the implicit datatype of a given column in a given dataframe
  
  Given: Dataframe; Columnname
   */
  def getDataType(dataframe: DataFrame, columnName: String) : String = {
    
    val amount = 10
    val datatypes = ArrayBuffer ("integer", "decimal", "float", "double", "string", "boolean", "dateTime")
    var datatype = ArrayBuffer.fill[Int](7){0}
    var examples = dataframe.limit(amount).select(columnName).collect
    
    var i = 0
    for(i <- 0 until examples.size) {
      
      var j = 0
      for(j <- 0 until 7)
        
        if(examples(i).apply(0).toString.contains(datatypes(j))) {
          datatype(j) += 1
        }
    }
    
    
    
    return datatypes(datatype.indexOf(datatype.max))
  }
  
    
}