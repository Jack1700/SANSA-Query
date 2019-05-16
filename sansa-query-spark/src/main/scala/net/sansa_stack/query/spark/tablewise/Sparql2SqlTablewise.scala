package net.sansa_stack.query.spark.tablewise

import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.syntax.ElementVisitorBase
import org.apache.jena.sparql.syntax.ElementWalker
import java.util.HashSet
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

  def for1Pattern(subject: String, predicate: String, _object: String, tableNum: Int): String = {

    var beforeWhere = false;
    var beforeSelect = false;
    var select = "SELECT ";
    val from = " FROM triples ";
    var where = " WHERE "

    
    if (subject(0) == '"') {
      where += " triples.s=" + subject
      beforeWhere = true
    } else {
      select +=" triples.s AS " + subject + " "
      beforeSelect = true

    }
    if (predicate(0) == '"') {
      if (beforeWhere)
        where += " AND "
      where += " triples.p=" + predicate

      beforeWhere = true
    } else {
      if (beforeSelect)//select coma here
        select+= " , "
      select += " triples.p AS " + predicate + " "
      beforeSelect=true

    }

    if (_object(0) == '"') {
      if (beforeWhere)
        where += " AND "
      where += " triples.o= " + _object;

    } else {
      if (beforeSelect)
        select+= " , "
      select += " triples.o AS " + _object + " "
    }
    return " ( (" + select + from + where + ")" + " AS " + "T" + tableNum + " ) "
  }
  
  

  def cleanProjectVariables(projectVariables: List[Var]): String = {
    var variables = new ArrayBuffer[String]();
    var v = 0;
    for (v <- 0 until projectVariables.size()) {
      print(v)
      variables += projectVariables.get(v).toString.substring(1,2);
    }
    return variables.toString.substring(12, variables.toString.size - 1);
  }
  
  

  def Sparql2SqlTablewise(QueryString: String): String = {
    val query = QueryFactory.create(QueryString);
    val myStack = initQueryStack(query);
    val mySubQuery:SubQuery = joinQueries(myStack);
    return mySubQuery.getQuery();

  }
  
  def initQueryStack(myQuery:Query): ArrayStack[SubQuery] = {
    var stack: ArrayStack[SubQuery] = new ArrayStack[SubQuery]();
    TripleGetter.generateStringTriples(myQuery);
    for (i <- 0 until TripleGetter.getSubjects().size) {
      val _newSubQuery:SubQuery = new SubQuery();
      
      val _subject = TripleGetter.getSubjects()(i);
      val _predicate = TripleGetter.getPredicates()(i);
      val _object = TripleGetter.getObjects()(i);
      
      _newSubQuery.setName("T" + i);
      _newSubQuery.setQuery(for1Pattern(_subject,_predicate,_object,i))
      
      //Prof Subject is a Variable
      if(_subject(0) !=  '"'){
        _newSubQuery.appendVariable(_subject);
      }
       //Prof Predicate is a Variable
      if(_predicate(0) !=  '"'){
        _newSubQuery.appendVariable(_predicate);
      }
       //Prof Object is a Variable
      if(_object(0) !=  '"'){
        _newSubQuery.appendVariable(_object);
      }
      stack.push(_newSubQuery);

    }
    return stack;
  }
  
  def joinQueries(myStack:ArrayStack[SubQuery]):SubQuery = {
    while(myStack.length > 1){
      myStack.push(JoinSubQuery.join(myStack.pop(), myStack.pop()))
    }
    return myStack.pop();
  }
  
 
/*
  def joinOn(lastSubject: String, lastPredicate: String, lastObject: String, subject: String, predicate: String, _object: String,
             tableNum1: Int, tableNum2: Int): String = {
    var joinStatement = " ON "
    var joined = false
    if (lastSubject(0) != '"') {
      lastSubject match {
        case subject   => joinStatement += tableNum1 + "." + lastSubject + "=" + tableNum2 + "." + subject + " "; joined = true;
        case predicate => joinStatement += tableNum1 + "." + lastSubject + "=" + tableNum2 + "." + predicate + " "; joined = true;
        case _object   => joinStatement += tableNum1 + "." + lastSubject + "=" + tableNum2 + "." + _object + " "; joined = true;
      }
    }

    if (lastPredicate(0) != '"') {
      lastPredicate match {
        case subject => {
          if (joined)
            joinStatement += " AND "
          joined = true;
          joinStatement += tableNum1 + "." + lastPredicate + "=" + tableNum2 + "." + subject + " ";
        }
        case predicate => {
          if (joined)
            joinStatement += " AND "
          joined = true;
          joinStatement += tableNum1 + "." + lastPredicate + "=" + tableNum2 + "." + predicate + " ";
        }
        case _object => {
          if (joined)
            joinStatement += " AND "
          joined = true;
          joinStatement += tableNum1 + "." + lastPredicate + "=" + tableNum2 + "." + _object + " ";
        }
      }
    }
    if (lastObject(0) != '"') {
      lastObject match {
        case subject => {
          if (joined)
            joinStatement += " AND "
          joinStatement += tableNum1 + "." + lastObject + "=" + tableNum2 + "." + subject + " ";
        }
        case predicate => {
          if (joined)
            joinStatement += " AND "
          joinStatement += tableNum1 + "." + lastObject + "=" + tableNum2 + "." + predicate + " ";

        }
        case _object => {
          if (joined)
            joinStatement += " AND "
          joinStatement += tableNum1 + "." + lastObject + "=" + tableNum2 + "." + _object + " ";

        }
      }
    }

    return  joinStatement
  }
*/
  
  def createQueryExecution(spark: SparkSession, sparqlQuery: String): DataFrame = {
    val sqlQuery = Sparql2SqlTablewise(sparqlQuery) // it will return the sql query
    print(sqlQuery)
    val df = spark.sql(sqlQuery)
    df
  }
}
