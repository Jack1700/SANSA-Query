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
    
    return JoinQueries2(queries,query.getProjectVars)
  }
  
  
  def for1Pattern(subject: String, predicate: String, _object: String, tableNum: Int, variables: HashSet[String]): String = {
    var beforeWhere = false;
    var beforeSelect = false;
    var select = "SELECT ";
    val From = " FROM triples ";
    var where = " WHERE "
    var whereUsed = false;

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
    
    if (!whereUsed) where = ""

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

  
  def cleanProjectVariables(projectVariables: List[Var],
      SubQuerys: Queue[SubQuery]): String = {
    
    var variables = new ArrayBuffer[String]();
    var v = 0;
    for (v <- 0 until projectVariables.size()) {
      var variable = projectVariables.get(v).toString;
      variable = variable.substring(1,variable.size)
      variables += methods.getTablewithVariable(variable, SubQuerys) + "." + variable
    }
    return variables.toString.substring(12, variables.toString.size - 1);
  }


  
  
  def JoinQueries2(queries: Queue[SubQuery], projectVariables: List[Var]): String = {
    var Statement = "SELECT " + 
    cleanProjectVariables(projectVariables, queries) + 
    " FROM \n";
    
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
        
        if ((containsVariable(variable, variables)!=null)){
          vFound = true
        }
      }
      if (vFound){
        
        Statement += "INNER JOIN " + 
                      Q.getQuery() + 
                      Q.getName() + 
                      onPart2(Q, variables) +
                      "\n";
      }
      else{
        queries.enqueue(Q)
      }
    }
    
    return Statement
    
  }
  
  def onPart2(query: SubQuery, variables: ArrayBuffer[String]): String = {
    var queryVariables = ArrayBuffer(query.getVariables().toArray: _*)
    var on = " ON "
    var onUsed = false;
    for (v <- queryVariables){
      var name = containsVariable(v,variables)
      
      if (name!= null){
        if (onUsed){
          on+= " AND "
        }
        
        on+= name + "." + v + " = " + query.getName() + "." + v
        onUsed= true
      } else{
        variables += (query.getName()+"."+v)
      }
    }
    return on
  }
  
  
  def containsVariable(variable: String, variables: ArrayBuffer[String]): String = {
    for (v <- variables){
      val str = v.split("\\.")
      if (variable == str(1)){
        return str(0)  
      }
    }
    return null
  }
  
  def JoinQueries(queries: Queue[SubQuery], projectVariables: List[Var]): String = {
    
    
    /*
     * 
     * Format must be like this
    ;WITH Data AS(Select * From Customers)

		SELECT
    *
		FROM
		    Data D1
		    INNER JOIN Data D2 ON D2.ID=D1.ID
    		INNER JOIN Data D3 ON D3.ID=D2.ID
    		INNER JOIN Data D4 ON D4.ID=D3.ID 
    		INNER JOIN Data D5 ON D5.ID=D4.ID
    		INNER JOIN Data D6 ON D6.ID=D5.ID
    		INNER JOIN Data D7 ON D7.ID=D6.ID
    *
    */

    var Statement = "SELECT " +
                    cleanProjectVariables(projectVariables, queries) +
                    " FROM \n";
    val Q0 = queries.dequeue()
    var variables = new ArrayBuffer[String]();
    Statement += Q0.getQuery() + " Q0 \n";
    var i = 1;
    for (i <- 0 until queries.size) {
      val Q = queries(i);
      if (i == 0) {
        Statement += "INNER JOIN " +
                      Q.getQuery() +
                      " Q1 " +
                      onPart(Q0, queries(i), "Q0", "Q1") +
                      "\n";
      } else {
        Statement += "INNER JOIN " +
                     Q.getQuery() +
                     " Q" + (i + 1) +
                     onPart(queries(i - 1), queries(i), "Q" + i, "Q" + (i + 1)) +
                     "\n";
      }
    }
    return Statement;
  }

  
  def onPart(q1: SubQuery, q2: SubQuery, name1: String, name2: String): String = {
    
    var joinVariables = q1.getVariables().intersect(q2.getVariables()).toList;
    var onPart = " ON "
    var i = 0;
    
    for (i <- 0 until joinVariables.size) {
      onPart += name1 + "." + joinVariables(i) + "=" + name2 + "." + joinVariables(i)
      if (i != joinVariables.size - 1) {
        onPart += " AND "
      }
    }

    return onPart;

  }

  
  def initializeQueryQueue(myQuery: Query): Queue[SubQuery] = {
    
    var queries: Queue[SubQuery] = new Queue[SubQuery]();
    
    SparqlAnalyzer.generateStringTriples(myQuery);
    SparqlAnalyzer.generateFilters(myQuery);
    
    for (i <- 0 until SparqlAnalyzer.subjects.size) {
      
      val _newSubQuery: SubQuery = new SubQuery();
      val _subject = SparqlAnalyzer.subjects(i);
      val _predicate = SparqlAnalyzer.predicates(i);
      val _object = SparqlAnalyzer.objects(i)
      
      //check Subject is a Variable
      if (_subject(0) != '"') {
        _newSubQuery.appendVariable(_subject);
      }
      
      //check Predicate is a Variable
      if (_predicate(0) != '"') {
        _newSubQuery.appendVariable(_predicate);
      }
      
      //check Object is a Variable
      if (_object(0) != '"') {
        _newSubQuery.appendVariable(_object);
      }
      
      _newSubQuery.setName("Q" + i);
      _newSubQuery.setQuery(for1Pattern(_subject,
                                        _predicate, 
                                        _object, 
                                        i,
                                        _newSubQuery.getVariables()
                                     )
                               )
      
      queries.enqueue(_newSubQuery);

    }
   
   
    return queries;
    
    
  }


  
  

  
}
