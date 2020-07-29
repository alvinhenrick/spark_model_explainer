package com.clarifyhealth.prediction.explainer

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

import scala.collection.{SortedMap, mutable}
import scala.util.Random

class DataGenerator extends QueryTest with SharedSparkSession {

  test("Test 1000") {
    spark.sharedState.cacheManager.clearCache()

    val wagesDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(getClass.getResource("/regression/cps_85_wages.csv").getPath)

    // wagesDF.show(truncate = false)

    val expression = wagesDF.columns.map(x => s"collect_set(${x}) as ${x}")

    val row = wagesDF.selectExpr(expression: _*).first()
    val wages = SortedMap(row.getValuesMap[Any](row.schema.fieldNames).toSeq: _*)

    val broadcastWages = spark.sparkContext.broadcast(wages)

    val df = spark.range(1, 10001)

    val outputSchema = StructType(StructField("id", LongType, false) +: wagesDF.schema.sortBy(x => x.name))

    val outDF = df.mapPartitions { rows =>
      val random = new Random()
      rows.map { id =>
        val d = broadcastWages.value.asInstanceOf[Map[String, mutable.WrappedArray[Any]]]
        Row.fromSeq(id +: d.values.map(x => x(random.nextInt(x.length))).toSeq)
      }
    }(RowEncoder.apply(outputSchema))

    val finalDF = outDF.select("id", wagesDF.columns: _*)

    finalDF.show(truncate = false)

    // finalDF.coalesce(1).write.option("header", "true").csv("/tmp/test1000")

  }
}
