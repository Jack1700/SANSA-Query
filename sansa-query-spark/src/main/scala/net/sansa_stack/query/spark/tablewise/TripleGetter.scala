package net.sansa_stack.query.spark.tablewise

import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.syntax.ElementVisitorBase
import org.apache.jena.sparql.syntax.ElementWalker
import java.util.HashSet
import scala.collection.mutable.ArrayBuffer
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }
import org.apache.jena.query.Query
import arq.query



object TripleGetter {

  private var subjects = new ArrayBuffer[String]();
  private var objects = new ArrayBuffer[String]();
  private var predicates = new ArrayBuffer[String]();
  
  def getSubjects(): ArrayBuffer[String] ={
    return subjects;
  }
  
  def getPredicates(): ArrayBuffer[String] ={
    return predicates;
  }
  
  def getObjects(): ArrayBuffer[String] ={
    return objects;
  }
  
  
  
  def generateStringTriples(query: Query):Unit = {
    subjects.clear
    predicates.clear
    objects.clear

    // This will walk through all parts of the query
    ElementWalker.walk(
      query.getQueryPattern(),
      // For each element...
      new ElementVisitorBase() {
        // ...when it's a block of triples...

        override def visit(el: ElementPathBlock): Unit = {
          // ...go through all the triples...
          val triples = el.patternElts();
          while (triples.hasNext()) {
            // ...and grab the subject
            val triple = triples.next();
            val subject = triple.getSubject().toString();
            if (subject(0) == '?') {
              subjects += subject.substring(1);
            } else {
              subjects += "\"" + subject + "\"";
            }
            val Object = triple.getObject().toString();
            if (Object(0) == '?') {
              objects += Object.substring(1);
            } else {
              objects += "\"" + Object + "\"";
            }
            val predicate = triple.getPredicate().toString();
            if (predicate(0) == '?') {
              predicates += predicate.substring(1);
            } else {
              predicates += "\"" + predicate + "\"";
            }
          }
        }
      });
  }
}