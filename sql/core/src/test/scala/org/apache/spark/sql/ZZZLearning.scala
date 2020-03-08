/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.Comparator

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.util.collection.AppendOnlyMap

case class Person(name: String, age: Int)

class ZZZLearning extends SparkFunSuite {
  // scalastyle:off println

  test("case class") {
    val spark = SparkSession.builder
      .config("spark.master", "local")
//      .config("spark.sql.codegen.wholeStage", "false")
      .getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize((1 to 2).map(i => Person(i.toString, i + 20)))
    val rawdf = rdd.toDF()
    val df = rawdf.select("name")
    val x = df.queryExecution.analyzed
    val y = df.queryExecution.optimizedPlan
    val z = df.queryExecution.sparkPlan
    val zz = df.queryExecution.executedPlan
//    println(x)

//    df.queryExecution.debug.toFile("xxxx.java")
    df.queryExecution.debug.codegen()
    val ret = df.rdd
  }

  test("person") {
    val spark = SparkSession.builder.config("spark.master", "local").getOrCreate()

    import spark.implicits._

    val caseClassDS = Seq(("Andy", 32)).toDF("name", "age")
    caseClassDS.show(1)
  }

  test("sql") {
    val spark = SparkSession.builder.config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext.parallelize(1 to 9, 1).toDF()
    df.show(10)
  }


  test("union") {
    val conf = new SparkConf()
      .set("spark.shuffle.compress", "false")
      .set("ark.shuffle.spill.compress", "false")
    //      .set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, 2)
    val sc = new SparkContext("local", "ShuffleLearning", conf)
    val u1 = sc.parallelize(List("c", "c", "p", "m", "t"))
    val u2 = sc.parallelize(List("c", "m", "k"))
    val result = u1.union(u2)
    result.collect()
  }

  test("join") {
    val conf = new SparkConf()
      .set("spark.shuffle.compress", "false")
      .set("spark.shuffle.spill.compress", "false")
    //      .set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, 2)
    val sc = new SparkContext("local", "ShuffleLearning", conf)
    val xxx = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 1)
    val yyy = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"),
      (4, "iPad"), (2, "banana")), 1)
    val rdd = xxx.fullOuterJoin(yyy)
    val resutl = rdd.collect
    resutl.foreach(println)
  }

  test("flatMapValues") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "mapValues", conf)

    val rdd = sc.parallelize(Seq((1, Seq(100, 101, 102)), (2, Seq(200, 201)), (3, Seq(300, 301))))
    val rdd2 = rdd.flatMapValues(v => v)
    val result = rdd2.collect()
    result.foreach(println)
  }

  test("mapValuesxxx") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "mapValues", conf)

    val rdd = sc.parallelize(Seq((1, 100), (2, 200), (3, 300), (1, 101), (1, 102)))
    val rdd2 = rdd.foldByKey(0)(_ + _)
    val result = rdd2.collect()
    result.foreach(println)
  }

  test("mapValues") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "mapValues", conf)

    val rdd = sc.parallelize(Seq((1, Seq(100, 101, 102)), (2, Seq(200, 201)), (3, Seq(300, 301))))
    val rdd2 = rdd.mapValues(v => v)
    val result = rdd2.collect()
    result.foreach(println)
  }

  test("cogroup") {

    val conf = new SparkConf()
      .set("spark.shuffle.compress", "false")
      .set("spark.shuffle.spill.compress", "false")
    //      .set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, 2)
    val sc = new SparkContext("local", "ShuffleLearning", conf)
    val xxx = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 1)
    val yyy = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"),
      (4, "iPad"), (2, "banana")), 1)
    val rdd = xxx.cogroup(yyy)
    val resutl = rdd.collect
    resutl.foreach(println)
  }

  test("Shuffle learning") {
    // scalastyle:off println
    val conf = new SparkConf()
      .set("spark.shuffle.compress", "false")
      .set("spark.shuffle.spill.compress", "false")
    //      .set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, 2)
    val sc = new SparkContext("local", "ShuffleLearning", conf)

    val rdd = sc.parallelize(10 until 19, 2).groupBy((x: Int) => {
      x % 3
    })

    val rdd1 = rdd.mapPartitions(iter => {
      val tc = TaskContext.get
      println("-------- Task ID [ " + tc.taskAttemptId() + " ]  begin --------")
      while (iter.hasNext) {
        val value = iter.next()
        println("key:" + value._1 + " values:" + value._2)
      }
      println("-------- Task ID [ " + tc.taskAttemptId() + " ]  done --------")
      Iterator.empty
    })
    val resultGroupby = rdd1.foreachPartition(() => _)
    //
    //    resultGroupby.foreach(x => println("key:" + x._1 + " value:" + x._2))

    //    val rdd = sc.textFile("/home/bobwang/xxx.csv", 1)
    //    rdd.repartition(8).count()


    //    val result = sc.parallelize(0 until 4)
    //      .map { i => (i / 2, i) }
    //       .reduceByKey(math.max).collect()

    // scalastyle:off println
    //    resultGroupby.foreach(x => println("x --- " + x))
    // scalastyle:on println

    //    val rdd = sc.parallelize(1 to 9)
    //    val rdd1 = rdd.repartition(3)
    //    val count = rdd1.count()
    //    assert(count == 7)
  }

  test("AppendOnlyMapSuite") {
    val map = new AppendOnlyMap[Int, Int](16)
    for (i <- 1 to 10) {
      map(i) = i + 100
    }
    val iter = map.toIterator
    while (iter.hasNext) {
      // scalastyle:off println
      val v = iter.next()
      println("key " + v._1 + " v:" + v._2)
      // scalastyle:on println
    }
    // scalastyle:off println
    println("---------------")

    val iter1 = map.destructiveSortedIterator(new HashComparator[Int])
    while (iter1.hasNext) {
      // scalastyle:off println
      val v = iter1.next()
      println("key " + v._1 + " v:" + v._2)
      // scalastyle:on println
    }
  }

  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }

}
