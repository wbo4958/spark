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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Ascending, AttributeReference, BoundReference, Descending, ExpressionEvalHelper, GenericInternalRow, GreaterThan, InputFileName, Literal, MutableProjection, Predicate, RowOrdering, SortOrder}
import org.apache.spark.sql.types._


class BobbyTestSuite extends SparkFunSuite with ExpressionEvalHelper {
  import org.apache.spark.sql.catalyst.dsl.expressions._
//  import org.apache.spark.sql.catalyst.dsl.plans._
  test("column") {
    val x = new Column("x")
  }
  test("Attribute") {
    val x = UnresolvedAttribute("a.b")
    val y = 'a as "a"
    val z: UnresolvedAttribute = 'a
    val zz = 'a.attr
    val xx = 'a.boolean
    xx.resolved
    val yy = rand(2)
    println(y)
  }

  test("InputFileName") {
    val x = InputFileName()
    val y = x.genCode(new CodegenContext)
    println(s"y = ${y}")
  }
  test("UnaryExpression") {
    val abs = Abs(Literal(-4))
    val input = new GenericInternalRow(Array[Any](1, 20))
    val x = abs.eval(input)
    println(x)

    val ctx = new CodegenContext
    val genCode = abs.genCode(ctx)
    println(s"y = ${genCode}")
  }

  test("Add") {
    val add = Add(Literal(4), Literal(5))
    val input = new GenericInternalRow(2)
    input.update(0, 1)
    input.update(1, 20)
    add.eval(input)

    val ctx = new CodegenContext
    val genCode = add.genCode(ctx)
    println(s"y = ${genCode}")
  }

  test("CodegenContext genExpressions") {
    // scalastyle:off println
    val ref = BoundReference(0, IntegerType, true)
    val ref1 = BoundReference(1, IntegerType, true)
    val add1 = Add(ref, ref1)
    val add2 = Add(add1, ref1)
    val ctx = new CodegenContext
    val x = ctx.generateExpressions(Seq(add2, add1), doSubexpressionElimination = false)
    println(ctx.declareMutableStates())
    println(ctx.declareAddedFunctions())
    println(ctx.subexprFunctionsCode)
    println(x)



    //    GenerateUnsafeProjection.generate(Seq(add2, add1), subexpressionEliminationEnabled = true)
    //    println("--- " + ctx.subexprFunctionsCode)
    //    println("--- " + ctx.declareAddedFunctions())


  }
  test("CodegenContext genGreater") {
    // scalastyle:off println
    val ctx = new CodegenContext
    println(ctx.genGreater(IntegerType, "1", "2"))

  }
  test("CodegenContext genEqual") {
    // scalastyle:off println
    val ctx = new CodegenContext
    println(ctx.genEqual(BooleanType, "true", "false"))
    println(ctx.genEqual(FloatType, "1", "2"))
    println(ctx.genEqual(DoubleType, "1", "2"))
    println(ctx.genEqual(IntegerType, "1", "2"))
    println(ctx.genEqual(BinaryType, "c1", "c2"))
    println(ctx.genEqual(StringType, "c1", "c2"))
    println(ctx.genEqual(NullType, "c1", "c2"))
    val schema = StructType(Seq(StructField("col1", IntegerType), StructField("col2", FloatType)))
    println(ctx.genComp(schema, "s1", "s2"))
    println(ctx.declareAddedFunctions())
    // scalastype:on println
  }

  test("CodegenContext genComp") {
    // scalastyle:off println
    val ctx = new CodegenContext
    println(ctx.genComp(BooleanType, "true", "false"))
    println(ctx.genComp(FloatType, "1", "2"))
    println(ctx.genComp(DoubleType, "1", "2"))
    println(ctx.genComp(IntegerType, "1", "2"))
    println(ctx.genComp(BinaryType, "c1", "c2"))
    println(ctx.genComp(StringType, "c1", "c2"))
    println(ctx.genComp(NullType, "c1", "c2"))
    val schema = StructType(Seq(StructField("col1", IntegerType), StructField("col2", FloatType)))
    println(ctx.genComp(schema, "s1", "s2"))
    println(ctx.declareAddedFunctions())

    println("---------------")
    println(ctx.genComp(ArrayType(IntegerType), "a1", "a2"))
    println(ctx.declareAddedFunctions())

    // scalastype:on println
  }

  class Foo {
    def showName(): Unit = {
      // scalastyle:off println
      println("my name is Foo")
      // scalastyle:on
    }
  }
  test("CodegenContext test") {
    // scalastyle:off println
    val ctx = new CodegenContext
    val foo = new Foo()

    val x = ctx.freshVariable("count", IntegerType)
    println(s"$x")

    val fooString = ctx.addReferenceObj("foo", foo)
    println(ctx.references.toArray)
    println(fooString)

    ctx.addNewFunction("sayHello1",
      """void sayHello1() {
        | System.out.println("say hello");
        |}
        |""".stripMargin)

    ctx.addNewFunction("sayHello2",
      """void sayHello2() {
        | System.out.println("say hello");
        |}
        |""".stripMargin)
    ctx.addNewFunction("sayHello3",
      """void sayHello3() {
        | System.out.println("say hello");
        |}
        |""".stripMargin)
    ctx.addNewFunction("sayHello4",
      """void sayHello4() {
        | System.out.println("say hello");
        |}
        |""".stripMargin)


    ctx.addInnerClass(
      s"""
         |public class DummyClass {
         |  public dummy() {}
         |}
           """.stripMargin)


    ctx.addMutableState(CodeGenerator.JAVA_INT, "count", v => s"$v = 0")
    ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "isApple", v => s"$v = true")
    ctx.addMutableState("Foo", "foo", v => s"$v = new Foo()")
    ctx.addMutableState("Foo", "foo_1", v => s"$v = new Foo()")
    ctx.addMutableState("Bar", "bar", v => s"$v = new Bar()")

    //
    //    ctx.addBufferedState(IntegerType, "name", "world")
    ctx.addPartitionInitializationStatement(s"partitionMaskTerm = ((long) partitionIndex) << 33;")
    //
    //    println(ctx.nullSafeExec(true, "valueIsNull")("value = 3"))
    //    println(ctx.nullArrayElementsSaveExec(true, "valueIsNull", "array")("value = 3"))

    println("--------------- inner class ----------")
    println(ctx.emitExtraCode())
    println("--------------- declared funcs ------")
    println(ctx.declareAddedFunctions())
    println("-------------- init partition ------------")
    println(ctx.initPartition())
    println("-------------- declared mutable states -----")
    println(ctx.declareMutableStates())
    println("-------------- init mutable states -----")
    println(ctx.initMutableStates())

    // scalastyle:on
  }

  test("literal gen code") {
    val lit = Literal(4)
    val ctx = new CodegenContext
    val x = lit.genCode(ctx)
    // scalastyle: off
    println(x)
    // scalastyle: on
  }

  test("BoundReference gen code") {
    val bound = BoundReference(0, IntegerType, true)
    val ctx = new CodegenContext
    val x = bound.genCode(ctx)
    // scalastyle: off
    println(s"x = ${x}")
    // scalastyle: on
  }

  test("generated projections basic") {
    val N = 1
    val wideRow1 = new GenericInternalRow((0 until N).toArray[Any])
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    //    val safeProj = SafeProjection.create(schema1)
    //    // test generated UnsafeProjection
    //    val unsafeProj = UnsafeProjection.create(schema1)

    val mutableProj = MutableProjection.create(Seq(BoundReference(0, IntegerType, true)))
  }

  test("test Ordering") {
    val c1 = AttributeReference("c1", IntegerType)()
    val c2 = AttributeReference("c2", IntegerType)()
    val c3 = AttributeReference("c3", IntegerType)()
    val input = Seq(c1, c2, c3)
    val order = Seq(SortOrder(c1, Ascending), SortOrder(c2, Descending))
    val ordering = RowOrdering.create(order, input)
  }

  test("test predicate") {
    val c1 = AttributeReference("c1", IntegerType)()
    val condition = GreaterThan(c1, Literal(3, IntegerType))
    val input = Seq(c1, AttributeReference("c2", IntegerType)())
    val predicate = Predicate.create(condition, input)
    //    val predicate = Predicate.create(EqualTo(Literal(2, IntegerType), Literal(3, IntegerType)))
    //    println(predicate)
    //    Predicate.create()
  }
}
