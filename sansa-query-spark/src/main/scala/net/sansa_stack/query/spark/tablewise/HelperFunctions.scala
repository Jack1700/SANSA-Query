package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.Queue
import org.apache.jena.sparql.core.Var
import scala.collection.mutable.ArrayBuffer
import java.util.List


class HelperFunctions {
  
  
  def cleanProjectVariables(projectVariables: List[Var], SubQuerys: Queue[SubQuery]): String = {
    
    var variables = new ArrayBuffer[String]()
    var v = 0;
    
    for (v <- 0 until projectVariables.size) {
      
      var variable = projectVariables.get(v).toString
      
      variable = variable.substring(1,variable.size)
      variables += getTablewithVariable(variable, SubQuerys) + "." + variable
    }
    
    return variables.toString.substring(12, variables.toString.size - 1);
  }  
  
  
  def getTablewithVariable (variable: String, SubQuerys: Queue[SubQuery]) : String = {
  
    var i = 0
    
    for (i <- 0 until SubQuerys.size) {
      if (SubQuerys(i).getVariables().contains(variable)) {
        return "Q" + i
      }
    }
    return "FunctionFailed"
  }
  
  
  def containsVariable(variable: String, variables: ArrayBuffer[String]): String = {
    
    for (v <- variables) {
      val str = v.split("\\.")
      
      if (variable == str(1)) {
        return str(0)  
      }
    }
    
    return null
  }
    
    
    
    
    
  
}