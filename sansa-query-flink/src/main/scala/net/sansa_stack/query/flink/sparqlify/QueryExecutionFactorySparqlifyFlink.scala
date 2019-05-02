package net.sansa_stack.query.flink.sparqlify

import org.aksw.jena_sparql_api.core.QueryExecutionFactoryBackQuery
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.jena.query.{Query, QueryExecution}

/**
  * Created by Simon Bin on 12/06/17.
  */
class QueryExecutionFactorySparqlifyFlink(val flinkEnv: ExecutionEnvironment, val flinkTable: BatchTableEnvironment, val sparqlSqlRewriter: SparqlSqlStringRewriter ) extends QueryExecutionFactoryBackQuery {
  override def getId: String = "flink"

  override def getState: String = flinkEnv.getIdString

  override def createQueryExecution(query: Query): QueryExecution = new QueryExecutionSparqlifyFlink(query, this, sparqlSqlRewriter, flinkEnv, flinkTable)

}
