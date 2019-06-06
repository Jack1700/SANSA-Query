package net.sansa_stack.query.spark.tablewise

import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.syntax.ElementVisitorBase
import org.apache.jena.sparql.syntax.ElementWalker
import org.apache.jena.sparql.syntax.ElementFilter
import java.util.HashSet
import scala.collection.mutable.ArrayBuffer
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }
import org.apache.jena.query.Query
import arq.query
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.syntax.ElementOptional

object SparqlAnalyzer {

  var subjects = new ArrayBuffer[String]();
  var objects = new ArrayBuffer[String]();
  var predicates = new ArrayBuffer[String]();
  var optionalSubjects = new ArrayBuffer[String]();
  var optionalObjects = new ArrayBuffer[String]();
  var optionalPredicates = new ArrayBuffer[String]();

  var filterVariables = new ArrayBuffer[String]();
  var filterValues = new ArrayBuffer[String]();
  var filterOperators = new ArrayBuffer[String]();

  /*
  Extracts all filters from the query
    
  Given: Sparql query
  */
  def generateFilters(query: Query) : Unit = {

    filterVariables.clear
    filterOperators.clear
    filterValues.clear
    ElementWalker.walk(
      query.getQueryPattern(),
      new ElementVisitorBase() {

        override def visit(element: ElementFilter): Unit = {

          var el = element.getExpr().toString()
          var variable = element.getExpr.getVarsMentioned.toString()

          filterOperators += el.charAt(1).toString();
          filterValues += el.split(" ")(2).substring(0, el.split(" ")(2).size - 1);
          filterVariables += variable.substring(2, variable.size - 1);
        }
      })
  }

 
  /*
  
    
  Given: 
  */
  def generateStringOptionalTriples(query: Query): Unit = {
    optionalSubjects.clear;
    optionalObjects.clear;
    optionalPredicates.clear;

    ElementWalker.walk(
      query.getQueryPattern(),
      new ElementVisitorBase() {

        override def visit(el: ElementOptional): Unit = {

          val triple = el.getOptionalElement
          println(triple)
          //            val subject = //triple.getSubject().toString()
          //            val Object = //triple.getObject().toString();
          //            val predicate = triple.getPredicate().toString();
          //
          //            if (subject(0) == '?')
          //              subjects += subject.substring(1)
          //            else
          //              subjects += "\"" + subject + "\""
          //
          //            if (Object(0) == '?')
          //              objects += Object.substring(1)
          //            else
          //              objects += "\"" + Object + "\""
          //
          //            if (predicate(0) == '?')
          //              predicates += predicate.substring(1)
          //            else
          //              predicates += "\"" + predicate + "\""
          //
          //          }
        }
      })
  }
  
  
  /*
  Extracts all triples from the query and saves them in three Arrays containing all subjects, objects and predicates from the BGPs 
    
  Given: Sparql query
  */
  def generateStringTriples(query: Query): Unit = {

    subjects.clear
    predicates.clear
    objects.clear

    ElementWalker.walk(
      query.getQueryPattern(),
      new ElementVisitorBase() {

        override def visit(el: ElementPathBlock): Unit = {

          val triples = el.patternElts()
          while (triples.hasNext()) {
            val triple = triples.next()
            val subject = triple.getSubject().toString()
            val Object = triple.getObject().toString();
            val predicate = triple.getPredicate().toString();

            if (subject(0) == '?')
              subjects += subject.substring(1)
            else
              subjects += "\"" + subject + "\""

            if (Object(0) == '?')
              objects += Object.substring(1)
            else
              objects += "\"" + Object + "\""

            if (predicate(0) == '?')
              predicates += predicate.substring(1)
            else
              predicates += "\"" + predicate + "\""

          }
        }
      })
  }

}