package net.sansa_stack.query.spark.tablewise

import scala.collection.mutable.Queue
import org.apache.jena.sparql.core.Var
import scala.collection.mutable.ArrayBuffer
import java.util.List
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

class HelperFunctions {

  /*
  Converts a project variable like ?x into a for SQL usable format like Q0.x where Q0 is the corresponding BGP

  Given: List of project variables; List of BGP's (as SubQueries)
  */
  def cleanProjectVariables(projectVariables: List[Var], SubQuerys: Queue[SubQuery]): String = {

    var variables = new ArrayBuffer[String]
    var v = 0

    for (v <- 0 until projectVariables.size) {

      var variable = projectVariables.get(v).toString

      variable = variable.substring(1, variable.size)
      variables += getBgpWithVariable(variable, SubQuerys) + "." + variable
    }

    return variables.toString.substring(12, variables.toString.size - 1)
  }

  /*
  Finds the number of the BGP where the variable first appears

  Given: a Variable as a String; List of BGP's (as SubQueries)
  */
  def getBgpWithVariable(variable: String, SubQuerys: Queue[SubQuery]): String = {

    var i = 0

    for (i <- 0 until SubQuerys.size) {
      if (SubQuerys(i).variables.contains(variable)) {
        return "Q" + i
      }
    }
    return "FunctionFailed"
  }

  /*
  Finds a variable in an Array of variables (like Q0.x) and returns it's prefix (like Q0)

  Given: Variable; Array of variables
  */
  def getTableWithVariable(variable: String, variables: ArrayBuffer[String]): String = {

    for (v <- variables) {
      val str = v.split("\\.")

      if (variable == str(1)) {
        return str(0)
      }
    }

    return null
  }

  /*
  Determines the implicit datatype of a given column in a given dataframe

  Given: Dataframe; Columnname
   */
  def getDataType(dataframe: DataFrame, columnName: String): String = {

    val amount = 10
    val datatypes = ArrayBuffer("integer", "decimal", "float", "double", "string", "boolean", "dateTime")
    var datatype = ArrayBuffer.fill[Int](7) { 0 }
    var examples = dataframe.limit(amount).select(columnName).collect

    var i = 0
    for (i <- 0 until examples.size) {

      var j = 0
      for (j <- 0 until 7)

        if (examples(i).apply(0).toString.contains(datatypes(j))) {
          datatype(j) += 1
        }
    }

    return datatypes(datatype.indexOf(datatype.max))
  }

  def mapToTypedDF(df: DataFrame, columnNames: ArrayBuffer[String], columnTypes: ArrayBuffer[String]): DataFrame = {

    import org.apache.spark.sql.functions._
    var df2 = df
    for (column <- columnNames) {
      val mySchema = df2.schema
      var c = df.withColumn("_tmp", split(col(column), "\\^")).select(
        col("_tmp").getItem(0).as(column)).drop("_tmp")
      val myList = c.select(column).rdd.map(r => r(0)).collect()
      val myListofStrings = myList.map(str => str.toString);
      var index = 0

      df2 = df2.drop(column)
      var rdd = df2.rdd

      var rdd2 = rdd.map((row: Row) => {
        var rowList = row.toSeq.toList;
        index += 1;
        val r = Row.fromSeq(rowList :+ myListofStrings(index))
        r
      })

      val spark = SparkSession.builder().getOrCreate()
      df2 = spark.createDataFrame(rdd2, mySchema)

    }

    // data types:  ("integer", "decimal", "float", "double", "string", "boolean", "dateTime")
    //CAST
    var i = 0
    for (i <- 0 until columnNames.size) {
      import org.apache.spark.sql.types._
      var myType:DataType = IntegerType
      columnTypes(i) match {
        case "integer"  => myType = IntegerType
        case "decimal"  => myType = DecimalType(5,2)
        case "float"    => myType = FloatType
        case "double"   => myType = DoubleType
        case "string"   => myType = StringType
        case "boolean"  => myType = BooleanType
        case "dateTime" => myType = TimestampType
      }
       println("VAAAAAAAALLLLLLUEEEEEEEEEEE IS "+ myType)
       val df3= df2.selectExpr("*")
       df2 = df3.withColumn("Tmp", df2.col(columnNames(i)).cast(myType))
        .drop(columnNames(i))
        .withColumnRenamed("Tmp", columnNames(i))
    }
    
    //ORDER BY
    for (i <- 0 until columnNames.size) {
      val index = columnNames.size - 1 - i
      df2 = df2.orderBy(columnNames(index))
    }
    df2
  }
}