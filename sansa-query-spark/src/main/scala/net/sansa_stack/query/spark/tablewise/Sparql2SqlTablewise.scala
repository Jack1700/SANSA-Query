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

  
  val methods = new HelperFunctions
  
  
  /*
  Takes a Sparql query in form of a string and returns the same query in Sql code
  
  Given: Sparql query string
  */
  def assembleQuery(queryString: String) : String = {
    
    val query = QueryFactory.create(queryString)
    val returnVariables = query.getProjectVars
    val queries = initializeQueryQueue(query)
    val select = generateSelect(query,queries, returnVariables)
    
    return select + JoinQueries(queries) + generateLimit(query)
  }
  
  
  /*
  Creates a queue of subqueries from a given Sparql query
  
  Given: Jena Sparql Query
  */
  def initializeQueryQueue(myQuery: Query) : Queue[SubQuery] = {
    
    var queries: Queue[SubQuery] = new Queue[SubQuery]
    
    SparqlAnalyzer.analyze(myQuery)
    
    for (i <- 0 until SparqlAnalyzer.subjects.size) {
      
      val newSubQuery: SubQuery = new SubQuery
      val subject = SparqlAnalyzer.subjects(i)
      val predicate = SparqlAnalyzer.predicates(i)
      val Object = SparqlAnalyzer.objects(i)
      
      if(SparqlAnalyzer.optionalIndices.contains(i)) {
        newSubQuery.isOptional = true
      }
      
      if (subject(0) != '"') {
        newSubQuery.variables += subject
      }
      
      if (predicate(0) != '"') {
        newSubQuery.variables += predicate
      }
      
      if (Object(0) != '"') {
        newSubQuery.variables += Object
      }
      
      newSubQuery.sName = ("Q" + i)
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
  
  
  /*
  Creates the join condition for a specific subquery and returns it as a string
  
  Given: Subquery; Table of variables, with their first occurrence
  */
  def generateJoinConditions(query: SubQuery, variables: ArrayBuffer[String]) : String = {
    
    var queryVariables = ArrayBuffer(query.variables.toArray: _*)
    var on = " ON "
    var onUsed = false
    
    for (v <- queryVariables) {
      
      var name = methods.getTableWithVariable(v, variables)
      
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
  
  
  /*
  Creates the limit part of the Sql query if needed
  
  Given: Jena Sparql Query
  */
  def generateLimit(myQuery: Query) : String = {
    
    var limit = ""
    
    if (myQuery.hasLimit) {
      limit = "LIMIT " + myQuery.getLimit
    }
    
    return limit
  }
  
  
  /*
  Creates the corresponding Select part of the Sql query
  
  Given: Jena Sparql Query; Queue of all Subqueries; List of variables, selected in main query
  */
  def generateSelect(query: Query, queries: Queue[SubQuery], projectVariables: List[Var]) : String = {
    
    var select = "SELECT "
    
    if (query.isDistinct) {
      select += "DISTINCT " 
    }
    
    select += methods.cleanProjectVariables(projectVariables, queries) + " FROM \n"
    
    return select
  }
  
  
  /*
  Translates a single BGP into Sql
  
  Given: Subject; Predicate; Object; Number that will be the Name of the current Subquery; List of Variables and their first occurrence
  */
  def translateSingleBgp(subject: String,
    predicate: String, Object: String, tableNum: Int,
    variables: HashSet[String]) : String = {
    
    var beforeWhere = false
    var beforeSelect = false
    var select = "SELECT "
    val from = " FROM triples "
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

    if (Object(0) == '"') {
      if (beforeWhere)
        where += " AND "
      where += " triples.o= " + Object
      whereUsed = true
    } else {
      if (beforeSelect)
        select += " , "
      select += " triples.o AS " + Object + " "
    }
    
    if (!whereUsed) {
      where = ""
    }

    val filterVariables = SparqlAnalyzer.filterVariables
    var i = 0
    
    for (i <- 0 until filterVariables.size) {
      
      if (variables.contains(filterVariables(i))) {
        
        var filterVariableColumn = ""

         if (filterVariables(i) == subject) filterVariableColumn = "CAST(triples.s AS float) "
         else if (filterVariables(i) == predicate) filterVariableColumn = "CAST(triples.p AS float) "
         else if (filterVariables(i) == Object) filterVariableColumn = "CAST(triples.o AS float) "

        if (!whereUsed) {
          where = "WHERE " + filterVariableColumn + " " + SparqlAnalyzer.filterOperators(i) + " " + SparqlAnalyzer.filterValues(i)
          whereUsed = true
        } else {
          where += " AND " + filterVariableColumn + " " + SparqlAnalyzer.filterOperators(i) + " " + SparqlAnalyzer.filterValues(i)
        }
      }
    }
    
    return "  (" + select + from + where + ")"
  }


  /*
  Creates the main body if the Sql query by chaining all subqueries together
  
  Given: Queue of all Subqueries; 
  */
  def JoinQueries(queries: Queue[SubQuery]) : String = {
    
    var Statement = " "
    val Q0 = queries.dequeue
    var variables = new ArrayBuffer[String]
    
    for (variable <- Q0.variables){
      variables += ("Q0." + variable)
    }
    Statement += Q0.sQuery + " Q0 \n"
    
    while (!queries.isEmpty) {
      
      var Q = queries.dequeue
      var vFound = false
      
      for (variable <- Q.variables){
        
        if (methods.getTableWithVariable(variable, variables) != null) {
          vFound = true
        }
      }
      
      if (vFound) {
        
        if (Q.isOptional)
          Statement += "LEFT JOIN "
        else
          Statement += "INNER JOIN "
          
        Statement +=  Q.sQuery + 
                      Q.sName + 
                      generateJoinConditions(Q, variables) +
                      "\n" }
      else {
        queries.enqueue(Q)
      }
    }
    
    return Statement
  }
  
  
}