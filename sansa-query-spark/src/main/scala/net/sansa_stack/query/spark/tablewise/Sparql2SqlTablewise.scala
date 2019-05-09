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

class Sparql2SqlTablewise {

  def for1Pattern(subject: String, predicate: String, _object: String, tableNum: Int): String = {

    var beforeWhere = false;
    var select = "SELECT ";
    val from = " FROM triples ";
    var where = " WHERE "

    select += " triples.s "
    if (subject(0) == '"') {
      where += " triples.s=" + subject
      beforeWhere = true
    } else {
      select += " AS " + subject + " "

    }
    select += ", triples.p "
    if (predicate(0) == '"') {
      if (beforeWhere)
        where += " And "
      where += " triples.p=" + predicate

      beforeWhere = true
    } else {
      select += " AS " + predicate + " "

    }
    select += ", triples.o "

    if (_object(0) == '"') {
      if (beforeWhere)
        where += " And "
      where += " triples.o= " + _object;

    } else {
      select += " AS " + _object + " "
    }
    return " ( (" + select + from + where + ")" + " AS " + "A" + tableNum + " ) "
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
    TripleGetter.generateStringTriples(query);
    print(query.getProjectVars());
    val variables = cleanProjectVariables(query.getProjectVars());
    val select = "SELECT " + "A0." + variables + " ";
    var from = "FROM ";
    var i = 0;
    for (i <- 0 until TripleGetter.getSubjects().size) {
      val subject = TripleGetter.getSubjects()(i);
      val predicate = TripleGetter.getPredicates()(i);
      val _object = TripleGetter.getObjects()(i);
      var addToFrom = ""
      if (i > 0) {
        val lastSubject = TripleGetter.getSubjects()(i - 1);
        val lastPredicate = TripleGetter.getPredicates()(i - 1);
        val lastObject = TripleGetter.getObjects()(i - 1);
        // from += "\n Join \n";
        // addToFrom = "\n" + joinOn(lastSubject, lastPredicate, lastObject, subject, predicate, _object, i - 1, i) + "\n"
        from += " Join ";
        addToFrom = "  " + joinOn(lastSubject, lastPredicate, lastObject, subject, predicate, _object, i - 1, i) + "  "
      }
      from += for1Pattern(subject, predicate, _object, i);
      from += addToFrom

    }
    return select + from;

  }

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

    return joinStatement
  }

  def createQueryExecution(spark: SparkSession, sparqlQuery: String): DataFrame = {
    val sqlQuery = Sparql2SqlTablewise(sparqlQuery) // it will return the sql query
    val df = spark.sql(sqlQuery)
    df
  }
}