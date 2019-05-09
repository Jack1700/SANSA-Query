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

class Sparql2SqlTablewise {

  def for1Pattern(subject: String, predicate: String, _object: String): String = {

    var beforeWhere = false;
    var select = "SELECT ";
    val from = "FROM Triples ";
    var where = "WHERE "

    
    select+= "Triples.Subject "
    if (subject(0) == '"') {
      where += "Triples.Subject=" + subject
      beforeWhere = true
    } else {
      select += "AS " + subject + " "

    }
    select += ", Triples.Predicate"
    if (predicate(0) == '"') {
      if (beforeWhere) {
        where += " And "
      }
      where += "Triples.Predicate=" + predicate

      beforeWhere = true
    } else {
      select += " AS " + predicate + " "

    }
    select += ", Triples.Object"

    if (_object(0) == '"') {
      if (beforeWhere) {
        where += " And"
      }
      where += "Triples.Object=" + _object;

    } else {
      select += " AS " + _object + " "
    }
    return "(" + select + from + where + ")"
  }

  def cleanProjectVariables(projectVariables: List[Var]): String = {
    var variables = new ArrayBuffer[String]();
    var v = 0;
    for (v <- 0 until projectVariables.size()) {
      variables += projectVariables.get(v).toString.substring(1);
    }
    return variables.toString.substring(12, variables.toString.size - 1);
  }

  def Sparql2SqlTablewise(QueryString: String): String = {
    val query = QueryFactory.create(QueryString);
    val tableName = "Triples";
    TripleGetter.generateStringTriples(query);

    val variables = cleanProjectVariables(query.getProjectVars());
    val select = "SELECT " + variables + " ";
    var from = "FROM\n";
    var i = 0;
    for (i <- 0 until TripleGetter.getSubjects().size) {
      val subject = TripleGetter.getSubjects()(i);
      val predicate = TripleGetter.getPredicates()(i);
      val _object = TripleGetter.getObjects()(i);
      from += for1Pattern(subject, predicate, _object);
      if (i < TripleGetter.getSubjects().size - 1) {
        from += "\n" + "JOIN\n";
        // TO DO : ADD JOIN CONDITION (ON)
      }

    }
    return select + from;

  }

}