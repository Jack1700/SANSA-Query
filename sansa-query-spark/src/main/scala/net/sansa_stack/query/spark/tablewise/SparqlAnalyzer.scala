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

  var subjects = new ArrayBuffer[String]
  var objects = new ArrayBuffer[String]
  var predicates = new ArrayBuffer[String]

  var orderByVariables = new ArrayBuffer[String]
  var optionalIndices = new ArrayBuffer[Integer]

  var filterVariables = new ArrayBuffer[String]
  var filterValues = new ArrayBuffer[String]
  var filterOperators = new ArrayBuffer[String]

  /*
  Analyzes the given query and makes sure, all the necessary Array's are initialized in the correct order

  Given: Jena Sparql Query
  */
  def analyze(query: Query): Unit = {
    generateStringTriples(query)
    generateFilter(query)
    generateOptionals(query)
    generateOBvariables(query)
  }
  /*
  Extracts all Order By variables in order and saves them in orderByVariables as Strings

  Given: Jena Sparql query
  */
  def generateOBvariables(query: Query): Unit = {
    orderByVariables.clear
    val orderConditions = query.getOrderBy
    if (orderConditions != null) {
      var i = 0
      for (i <- 0 until orderConditions.size) {
        orderByVariables += orderConditions.get(i).getExpression.toString.substring(1)
      }

    }

  }
  /*
  Extracts all triples from the query and saves them in three Arrays containing all subjects, objects and predicates from the BGPs

  Given: Jena Sparql query
  */
  def generateStringTriples(query: Query): Unit = {

    subjects.clear
    predicates.clear
    objects.clear

    ElementWalker.walk(
      query.getQueryPattern,
      new ElementVisitorBase {

        override def visit(el: ElementPathBlock): Unit = {

          val triples = el.patternElts
          while (triples.hasNext) {
            val triple = triples.next
            val subject = triple.getSubject.toString
            val Object = triple.getObject.toString
            val predicate = triple.getPredicate.toString

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

  /*
  Extracts all filters from the query

  Given: Jena Sparql query
  */
  def generateFilter(query: Query): Unit = {

    filterVariables.clear
    filterOperators.clear
    filterValues.clear
    ElementWalker.walk(
      query.getQueryPattern,
      new ElementVisitorBase {

        override def visit(element: ElementFilter): Unit = {

          var el = element.getExpr.toString
          var variable = element.getExpr.getVarsMentioned.toString

          filterOperators += el.charAt(1).toString
          filterValues += el.split(" ")(2).substring(0, el.split(" ")(2).size - 1)
          filterVariables += variable.substring(2, variable.size - 1)
        }
      })
  }

  /*
  Determines, which BGP's are part of an optional

  Given: Jena Sparql Query
  */
  def generateOptionals(query: Query): Unit = {

    optionalIndices.clear

    ElementWalker.walk(
      query.getQueryPattern,
      new ElementVisitorBase {

        override def visit(el: ElementOptional): Unit = {

          val tripleOptional = el.getOptionalElement.toString
          val triple = tripleOptional.split(" ")

          var optionalSubject = triple(1)
          var optionalPredicate = triple(3)
          var optionalObject = triple(5)

          if (optionalSubject(0) == '?')
            optionalSubject = optionalSubject.substring(1)
          else
            optionalSubject = "\"" + optionalSubject.substring(1, optionalSubject.size - 1) + "\""

          if (optionalObject(0) == '?')
            optionalObject = optionalObject.substring(1)
          else
            optionalObject = "\"" + optionalObject.substring(1, optionalObject.size - 1) + "\""

          if (optionalPredicate(0) == '?')
            optionalPredicate = optionalPredicate.substring(1)
          else
            optionalPredicate = "\"" + optionalPredicate.substring(1, optionalPredicate.size - 1) + "\""

          var i = 0
          for (i <- 0 until subjects.size) {
            if (subjects(i) == optionalSubject && objects(i) == optionalObject && predicates(i) == optionalPredicate) {
              optionalIndices += i
            }
          }
        }
      })
  }

}