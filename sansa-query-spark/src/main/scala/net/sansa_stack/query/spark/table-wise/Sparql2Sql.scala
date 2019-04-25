import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.syntax.ElementVisitorBase
import org.apache.jena.sparql.syntax.ElementWalker
import java.util.HashSet
import scala.collection.mutable.ArrayBuffer
import org.apache.jena.query.{ QueryExecutionFactory, QuerySolutionMap, QueryFactory }

object Test extends App {

  def getStringTriples(QueryString: String): ArrayBuffer[ArrayBuffer[String]] = {
    val query = QueryFactory.create(QueryString);
    var buff = new ArrayBuffer[ArrayBuffer[String]]();
    var subjects = new ArrayBuffer[String]();
    var objects = new ArrayBuffer[String]();
    var predicates = new ArrayBuffer[String]();
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
              subjects += "\""+subject+"\"";
            }
            val Object = triple.getObject().toString();
            if (Object(0) == '?') {
              objects += Object.substring(1);
            } else {
              objects += "\""+Object+"\"";
            }
            val predicate = triple.getPredicate().toString();
            if (predicate(0) == '?') {
              predicates += predicate.substring(1);
            } else {
              predicates += "\""+predicate+"\"";
            }

          }
        }
      });

    buff += subjects;
    buff += predicates;
    buff += objects;
    return buff;

  }

  def SQLBuilder(tableName: String, QueryString: String): String = {
    val select = "SELECT * ";
    val from = "FROM " + tableName + " ";

    var TripleStrings = getStringTriples(QueryString);
    val subject = TripleStrings(0)(0);
    val predicate = TripleStrings(1)(0);
    val _object = TripleStrings(2)(0);

    val where = "WHERE " + "subject==" + subject + " And " + "predicate==" + predicate + " And " + "object==" + _object;

    return select + from + where;

  }

  val QueryString = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/> SELECT * WHERE {    ?person foaf:name ?name .    ?person foaf:mbox ?email .}";
  val sqlQuery = SQLBuilder("Triples", QueryString);
  println(sqlQuery);

}