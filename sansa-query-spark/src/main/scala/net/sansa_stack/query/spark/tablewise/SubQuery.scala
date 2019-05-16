package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.HashSet


class SubQuery{
  
  
  var variables: HashSet[String] = new HashSet[String];
  var sName : String = "";
  var sQuery: String = "";
  
  def appendVariable(variable:String):Unit = {
    variables += variable;
  }
  
  def setVariables(myVariables:HashSet[String]):Unit = {
    this.variables = myVariables;
  }
  
  def getVariables(): HashSet[String] = {
    return variables;
  }
  
  def setName(name: String):Unit = {
    this.sName = name;
  }
  
  def getName(): String = {
    return sName;
  }
  
  def setQuery(query:String):Unit = {
    this.sQuery = query;
  }
  
  def getQuery():String = {
    return sQuery;
  }
  
  
}

	
object JoinSubQuery {
    
  
 
  def join(q1: SubQuery, q2: SubQuery):SubQuery = {
    val newQuery: SubQuery = new SubQuery();
    newQuery.setName(q1.getName() + q2.getName());
    newQuery.setVariables(q1.getVariables().union(q2.getVariables()));
    
    //Build JOIN
    var joinVariables = q1.getVariables().intersect(q2.getVariables()).toList;
    var sQuery = "(" + q1.getQuery() + " JOIN " + q2.getQuery() + " ON " 
    var i:Int = 0;
    var len = joinVariables.size
    sQuery += "("
    //Build ON 
    for(i<- 0 until len) {
      sQuery += q1.getName() + "." + joinVariables(i) + "=" +  q2.getName() + "." + joinVariables(i)
      if(i != len-1){
        sQuery += " AND "
      }
    }
    //Build AS
    sQuery += ")" +" AS " + newQuery.getName() + ")";
    newQuery.setQuery(sQuery);
    
    return newQuery;
  }
}