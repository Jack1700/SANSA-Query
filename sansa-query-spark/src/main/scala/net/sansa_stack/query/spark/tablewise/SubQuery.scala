package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.HashSet
class SubQuery {

  var variables: HashSet[String] = new HashSet[String];
  var sName: String = "";
  var sQuery: String = "";

  def appendVariable(variable: String): Unit = {
    variables += variable;
  }

  def setVariables(myVariables: HashSet[String]): Unit = {
    this.variables = myVariables;
  }

  def getVariables(): HashSet[String] = {
    return variables;
  }

  def setName(name: String): Unit = {
    this.sName = name;
  }

  def getName(): String = {
    return sName;
  }

  def setQuery(query: String): Unit = {
    this.sQuery = query;
  }

  def getQuery(): String = {
    return sQuery;
  }

}
