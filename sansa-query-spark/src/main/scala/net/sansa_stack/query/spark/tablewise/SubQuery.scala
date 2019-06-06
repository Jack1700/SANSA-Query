package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.HashSet

class SubQuery{
  
  var variables: HashSet[String] = new HashSet[String]
  var sName : String = ""
  var sQuery: String = ""
  var isOptional = false
  
}
