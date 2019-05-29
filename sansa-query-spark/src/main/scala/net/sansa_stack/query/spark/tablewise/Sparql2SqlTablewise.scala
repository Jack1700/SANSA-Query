package net.sansa_stack.query.spark.tablewise

import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.syntax.ElementVisitorBase
import org.apache.jena.sparql.syntax.ElementWalker
import org.apache.jena.query.Query
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }
import org.apache.jena.sparql.core.Var
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashSet
import scala.collection.mutable.Set
import java.util.List
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayStack
import scala.collection.mutable.Queue
import org.jgrapht.alg.scoring.BetweennessCentrality.MyQueue


class Sparql2SqlTablewise {

  
  val methods = new HelperFunctions()
  
  
  def assembleQuery(queryString: String) : String = {
    
    val query = QueryFactory.create(queryString)
    val queries = initializeQueryQueue(query)
    val select = generateSelect(query,queries,query.getProjectVars)
    
    return select + JoinQueries(queries,query.getProjectVars)
  }
  
  
  def initializeQueryQueue(myQuery: Query) : Queue[SubQuery] = {
    
    var queries: Queue[SubQuery] = new Queue[SubQuery]()
    
    SparqlAnalyzer.generateStringTriples(myQuery)
    SparqlAnalyzer.generateFilters(myQuery)
    
    for (i <- 0 until SparqlAnalyzer.subjects.size) {
      
      val newSubQuery: SubQuery = new SubQuery()
      val subject = SparqlAnalyzer.subjects(i)
      val predicate = SparqlAnalyzer.predicates(i)
      val Object = SparqlAnalyzer.objects(i)
      
      if (subject(0) != '"') {
        newSubQuery.variables += subject
      }
      
      if (predicate(0) != '"') {
        newSubQuery.variables += predicate
      }
      
      if (Object(0) != '"') {
        newSubQuery.variables += Object
      }
      
      newSubQuery.sName = "Q" + i
      newSubQuery.sQuery = translateSingleBgp(subject,
                                       predicate, 
                                       Object, 
                                       i,
                                       newSubQuery.variables
                                     )
      queries.enqueue(newSubQuery)
    }
   
    return queries
  }
  
    
  def generateJoinConditions(query: SubQuery, variables: ArrayBuffer[String]) : String = {
    
    var queryVariables = ArrayBuffer(query.getVariables().toArray: _*)
    var on = " ON "
    var onUsed = false
    
    for (v <- queryVariables) {
      
      var name = methods.containsVariable(v, variables)
      
      if (name != null) {
        
        if (onUsed) {
          on += " AND "
        }
        
        on += name + "." + v + " = " + query.sName + "." + v
        onUsed = true
      } else {
        variables += query.sName + "." + v
      }
    }
    
    return on
  }
  
  
  def generateSelect(query: Query, queries: Queue[SubQuery], projectVariables: List[Var]) : String = {
    
    var mySelect = " " +
      methods.cleanProjectVariables(projectVariables, queries) + 
      " FROM \n";
    
    if (query.hasLimit())
      return "SELECT TOP " + query.getLimit().toString() + mySelect
    else
      return "SELECT " + mySelect
  }
  
  
  def translateSingleBgp(subject: String,
    predicate: String, _object: String, tableNum: Int,
    variables: HashSet[String]) : String = {
    
    var beforeWhere = false
    var beforeSelect = false
    var select = "SELECT "
    val From = " FROM triples "
    var where = " WHERE "
    var whereUsed = false

    if (subject(0) == '"') {
      where += " triples.s=" + subject
      beforeWhere = true
      whereUsed = true
    } else {
      select += " triples.s AS " + subject + " "
      beforeSelect = true
    }

    if (predicate(0) == '"') {
      if (beforeWhere)
        where += " AND "
      where += " triples.p=" + predicate
      whereUsed = true
      beforeWhere = true
    } else {
      if (beforeSelect) //select coma here
        select += " , "
      select += " triples.p AS " + predicate + " "
      beforeSelect = true

    }

    if (_object(0) == '"') {
      if (beforeWhere)
        where += " AND "
      where += " triples.o= " + _object;
      whereUsed = true
    } else {
      if (beforeSelect)
        select += " , "
      select += " triples.o AS " + _object + " "
    }
    
    if (!whereUsed) {
      where = ""
    }

    val filterVariables = SparqlAnalyzer.filterVariables
    var i = 0
    
    for(i <- 0 until filterVariables.size) {
      
      if(variables.contains(filterVariables(i))) {
        
        var filterVariableColumn = ""

         if (filterVariables(i)== subject) filterVariableColumn="CAST(triples.s AS float) "
         else if (filterVariables(i)== predicate) filterVariableColumn="CAST(triples.p AS float) "
         else if (filterVariables(i)== _object) filterVariableColumn="CAST(triples.o AS float) "

       
        if(!whereUsed) {
          where = "WHERE " + filterVariableColumn + " " + SparqlAnalyzer.filterOperators(i) + " " + SparqlAnalyzer.filterValues(i)
          whereUsed = true
        } else {
          where+= " AND "+ filterVariableColumn + " " + SparqlAnalyzer.filterOperators(i) + " " + SparqlAnalyzer.filterValues(i)
        }
      }
    }
    
    
    
    return "  (" + select + From + where + ")"
  }




  
  
  def JoinQueries(queries: Queue[SubQuery], projectVariables: List[Var]): String = {
    var Statement = " "
    val Q0 = queries.dequeue()
    var variables = new ArrayBuffer[String]()
    
    for (variable <- Q0.getVariables()){
      variables += ("Q0."+variable)
    }
    Statement += Q0.getQuery() + " Q0 \n";
    
    while(!queries.isEmpty){
      var Q = queries.dequeue()
      var vFound = false
      for (variable <- Q.getVariables()){
        
        if ((methods.containsVariable(variable, variables)!=null)){
          vFound = true
        }
      }
      if (vFound){
        
        Statement += "INNER JOIN " + 
                      Q.getQuery() + 
                      Q.getName() + 
                      generateJoinConditions(Q, variables) +
                      "\n";
      }
      else{
        queries.enqueue(Q)
      }
    }
    
    return Statement
    
  }

  
  
  
  
  
}
