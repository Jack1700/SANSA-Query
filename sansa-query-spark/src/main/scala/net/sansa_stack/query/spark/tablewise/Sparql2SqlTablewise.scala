package net.sansa_stack.query.spark.tablewise

import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.syntax.ElementVisitorBase
import org.apache.jena.sparql.syntax.ElementWalker
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }
import scala.collection.mutable.Set
import org.apache.jena.sparql.core.Var
import java.util.List
import org.apache.spark.sql.SparkSession
import org.apache.jena.query.Query
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.Stack
import scala.collection.mutable.ArrayStack



class Sparql2SqlTablewise {

  
  def for1Pattern(subject: String, predicate: String, _object: String, tableNum: Int, variables: HashSet[String]): String = {

    var beforeWhere = false;
    var beforeSelect = false;
    var select = "SELECT ";
    val from = " FROM triples ";
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

    
    val filterVariables = TripleGetter.getFilterVariables()
    var i = 0
    for(i <- 0 until filterVariables.size) {
      if(variables.contains(filterVariables(i))) {
        if(!whereUsed) {
          where = "WHERE " + filterVariables(i) + " " + TripleGetter.getFilterOperators()(i) + " " + TripleGetter.getFilterValues()(i)
          whereUsed = true
        } else {
          
        }
      }
    }
    
    
    
    return "  (" + select + from + where + ")"
  }

  
  def cleanProjectVariables(projectVariables: List[Var], SubQuerys: ArrayBuffer[SubQuery]): String = {
    var variables = new ArrayBuffer[String]();
    var v = 0;
    for (v <- 0 until projectVariables.size()) {
      val variable = projectVariables.get(v).toString.substring(1, 2);
      variables += getTablewithVariable(variable, SubQuerys) + "." + variable
    }
    return variables.toString.substring(12, variables.toString.size - 1);
  }

  
  def Sparql2SqlTablewise(QueryString: String): String = {
    val query = QueryFactory.create(QueryString);
    val queries = initQueryArray(query);
    return JoinQueries(queries,query.getProjectVars);

  }

  
  def JoinQueries(queries: ArrayBuffer[SubQuery], projectVariables: List[Var]): String = {
    
    
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

    var Statement = "SELECT " + cleanProjectVariables(projectVariables, queries) + " FROM \n";
    val Q0 = queries(0)
     queries.remove(0)
    Statement += Q0.getQuery() + " Q0 \n";
    var i = 1;
    for (i <- 0 until queries.size) {
      val Q = queries(i);
      if (i == 0) {
        Statement += "INNER JOIN " + Q.getQuery() + " Q1 " + onPart(Q0, queries(i), "Q0", "Q1") + "\n";
      } else {
        Statement += "INNER JOIN " + Q.getQuery() + " Q" + (i + 1) + onPart(queries(i - 1), queries(i), "Q" + i, "Q" + (i + 1)) + "\n";
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

  
  def initQueryArray(myQuery: Query): ArrayBuffer[SubQuery] = {
    var queries: ArrayBuffer[SubQuery] = new ArrayBuffer[SubQuery]();
    TripleGetter.generateStringTriples(myQuery);
    for (i <- 0 until TripleGetter.getSubjects().size) {
      val _newSubQuery: SubQuery = new SubQuery();

      val _subject = TripleGetter.getSubjects()(i);
      val _predicate = TripleGetter.getPredicates()(i);
      val _object = TripleGetter.getObjects()(i);

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
      
      _newSubQuery.setName("T" + i);
      _newSubQuery.setQuery(for1Pattern(_subject, _predicate, _object, i, _newSubQuery.getVariables()))

      }
      queries+= _newSubQuery;

    }
   
   
    return queries;
    
    
  }

  
  def createQueryExecution(spark: SparkSession, sparqlQuery: String): DataFrame = {
    val sqlQuery = Sparql2SqlTablewise(sparqlQuery) // it will return the sql query
    println(sqlQuery)
    val df = spark.sql(sqlQuery)
    df
  }
  
  
  def getTablewithVariable (variable: String, SubQuerys: ArrayBuffer[SubQuery]): String = {
    
    var i=0
    for (i <- 0 until SubQuerys.size) {
      if(SubQuerys(i).getVariables().contains(variable)) {
        return "Q" + i
      }
      
    }
    return ""
  }
  
}