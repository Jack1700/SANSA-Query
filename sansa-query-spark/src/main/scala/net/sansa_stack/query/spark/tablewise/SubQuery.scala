package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.HashSet


class SubQuery{
  
  var variables: HashSet[String] = new HashSet[String];
  var sName : String = "";
  var sQuery: String = "";
}

	
object JoinSubQuery {
    
 
  /*
  Join two subqueries
    
  Given: two subqueries
  */
  def join(q1: SubQuery, q2: SubQuery) : SubQuery = {
    
    val newQuery = new SubQuery()
    
    newQuery.sName = q1.sName + q2.sName
    newQuery.variables = q1.variables.union(q2.variables)
    
    var joinVariables = q1.variables.intersect(q2.variables).toList
    var sQuery = "(" + q1.sQuery + " JOIN " + q2.sQuery + " ON " 
    
    var i = 0
    var len = joinVariables.size
    sQuery += "("
    
    for (i<- 0 until len) {
      
      sQuery += q1.sName + "." + joinVariables(i) + "=" +  q2.sName + "." + joinVariables(i)
      
      if (i != len-1) {
        sQuery += " AND "
      }
    }

    sQuery += ")" + " AS " + newQuery.sName + ")"
    newQuery.sQuery = sQuery
    
    return newQuery
  }
  
}
