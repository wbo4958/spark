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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.ArrayType

/**
 * A test suite for generated projections
 */
class GeneratedProjectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("Attribute") {
    val x = UnresolvedAttribute("a.b")
    val y = 'a
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

  test("generated projections on wider table") {
    val N = 1000
    val wideRow1 = new GenericInternalRow((0 until N).toArray[Any])
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      (0 until N).map(i => UTF8String.fromString(i.toString)).toArray[Any])
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    // test generated UnsafeProjection
    val unsafeProj = UnsafeProjection.create(nestedSchema)
    val unsafe: UnsafeRow = unsafeProj(nested)
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === unsafe.getInt(i + 2))
      assert(s === unsafe.getUTF8String(i + 2 + N))
      assert(i === unsafe.getStruct(0, N * 2).getInt(i))
      assert(s === unsafe.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === unsafe.getStruct(1, N * 2).getInt(i))
      assert(s === unsafe.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated SafeProjection
    val safeProj = SafeProjection.create(nestedSchema)
    val result = safeProj(unsafe)
    // Can't compare GenericInternalRow with JoinedRow directly
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === result.getInt(i + 2))
      assert(s === result.getUTF8String(i + 2 + N))
      assert(i === result.getStruct(0, N * 2).getInt(i))
      assert(s === result.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === result.getStruct(1, N * 2).getInt(i))
      assert(s === result.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }

  test("SPARK-18016: generated projections on wider table requiring class-splitting") {
    val N = 4000
    val wideRow1 = new GenericInternalRow((0 until N).toArray[Any])
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      (0 until N).map(i => UTF8String.fromString(i.toString)).toArray[Any])
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    // test generated UnsafeProjection
    val unsafeProj = UnsafeProjection.create(nestedSchema)
    val unsafe: UnsafeRow = unsafeProj(nested)
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === unsafe.getInt(i + 2))
      assert(s === unsafe.getUTF8String(i + 2 + N))
      assert(i === unsafe.getStruct(0, N * 2).getInt(i))
      assert(s === unsafe.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === unsafe.getStruct(1, N * 2).getInt(i))
      assert(s === unsafe.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated SafeProjection
    val safeProj = SafeProjection.create(nestedSchema)
    val result = safeProj(unsafe)
    // Can't compare GenericInternalRow with JoinedRow directly
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === result.getInt(i + 2))
      assert(s === result.getUTF8String(i + 2 + N))
      assert(i === result.getStruct(0, N * 2).getInt(i))
      assert(s === result.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === result.getStruct(1, N * 2).getInt(i))
      assert(s === result.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }

  test("generated unsafe projection with array of binary") {
    val row = InternalRow(
      Array[Byte](1, 2),
      new GenericArrayData(Array(Array[Byte](1, 2), null, Array[Byte](3, 4))))
    val fields = (BinaryType :: ArrayType(BinaryType) :: Nil).toArray[DataType]

    val unsafeProj = UnsafeProjection.create(fields)
    val unsafeRow: UnsafeRow = unsafeProj(row)
    assert(java.util.Arrays.equals(unsafeRow.getBinary(0), Array[Byte](1, 2)))
    assert(java.util.Arrays.equals(unsafeRow.getArray(1).getBinary(0), Array[Byte](1, 2)))
    assert(unsafeRow.getArray(1).isNullAt(1))
    assert(unsafeRow.getArray(1).getBinary(1) === null)
    assert(java.util.Arrays.equals(unsafeRow.getArray(1).getBinary(2), Array[Byte](3, 4)))

    val safeProj = SafeProjection.create(fields)
    val row2 = safeProj(unsafeRow)
    assert(row2 === row)
  }

  test("padding bytes should be zeroed out") {
    val types = Seq(BooleanType, ByteType, ShortType, IntegerType, FloatType, BinaryType,
      StringType)
    val struct = StructType(types.map(StructField("", _, true)))
    val fields = Array[DataType](StringType, struct)
    val unsafeProj = UnsafeProjection.create(fields)

    val innerRow = InternalRow(false, 1.toByte, 2.toShort, 3, 4.0f,
      "".getBytes(StandardCharsets.UTF_8),
      UTF8String.fromString(""))
    val row1 = InternalRow(UTF8String.fromString(""), innerRow)
    val unsafe1 = unsafeProj(row1).copy()
    // create a Row with long String before the inner struct
    val row2 = InternalRow(UTF8String.fromString("a_long_string").repeat(10), innerRow)
    val unsafe2 = unsafeProj(row2).copy()
    assert(unsafe1.getStruct(1, 7) === unsafe2.getStruct(1, 7))
    val unsafe3 = unsafeProj(row1).copy()
    assert(unsafe1 === unsafe3)
    assert(unsafe1.getStruct(1, 7) === unsafe3.getStruct(1, 7))
  }

  test("MutableProjection should not cache content from the input row") {
    val mutableProj = GenerateMutableProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))
    val row = new GenericInternalRow(1)
    mutableProj.target(row)

    val unsafeProj = GenerateUnsafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))
    val unsafeRow = unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("a"))))

    mutableProj.apply(unsafeRow)
    assert(row.getStruct(0, 1).getString(0) == "a")

    // Even if the input row of the mutable projection has been changed, the target mutable row
    // should keep same.
    unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("b"))))
    assert(row.getStruct(0, 1).getString(0).toString == "a")
  }

  test("SafeProjection should not cache content from the input row") {
    val safeProj = GenerateSafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))

    val unsafeProj = GenerateUnsafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))
    val unsafeRow = unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("a"))))

    val row = safeProj.apply(unsafeRow)
    assert(row.getStruct(0, 1).getString(0) == "a")

    // Even if the input row of the mutable projection has been changed, the target mutable row
    // should keep same.
    unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("b"))))
    assert(row.getStruct(0, 1).getString(0).toString == "a")
  }

  test("SPARK-22699: GenerateSafeProjection should not use global variables for struct") {
    val safeProj = GenerateSafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", IntegerType), true)))
    val globalVariables = safeProj.getClass.getDeclaredFields
    // We need always 3 variables:
    // - one is a reference to this
    // - one is the references object
    // - one is the mutableRow
    assert(globalVariables.length == 3)
  }

  test("SPARK-18016: generated projections on wider table requiring state compaction") {
    val N = 6000
    val wideRow1 = new GenericInternalRow(new Array[Any](N))
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      Array.tabulate[Any](N)(i => UTF8String.fromString(i.toString)))
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    val safeProj = SafeProjection.create(nestedSchema)
    val result = safeProj(nested)

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }

  test("SPARK-33473: subexpression elimination for interpreted SafeProjection") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(
        SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> enabled,
        SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
        val one = BoundReference(0, DoubleType, true)
        val two = BoundReference(1, DoubleType, true)

        val mul = Multiply(one, two)
        val mul2 = Multiply(mul, mul)
        val sqrt = Sqrt(mul2)
        val sum = Add(mul2, sqrt)

        val proj = SafeProjection.create(Seq(sum))
        val result = (d1: Double, d2: Double) =>
          ((d1 * d2) * (d1 * d2)) + Math.sqrt((d1 * d2) * (d1 * d2))

        val inputRows = Seq(
          InternalRow.fromSeq(Seq(1.0, 2.0)),
          InternalRow.fromSeq(Seq(2.0, 3.0)),
          InternalRow.fromSeq(Seq(1.0, null)),
          InternalRow.fromSeq(Seq(null, 2.0)),
          InternalRow.fromSeq(Seq(3.0, 4.0)),
          InternalRow.fromSeq(Seq(null, null))
        )
        val expectedResults = Seq(
          result(1.0, 2.0),
          result(2.0, 3.0),
          null,
          null,
          result(3.0, 4.0),
          null
        )

        inputRows.zip(expectedResults).foreach { case (inputRow, expected) =>
          val projRow = proj.apply(inputRow)
          if (expected != null) {
            assert(projRow.getDouble(0) == expected)
          } else {
            assert(projRow.isNullAt(0))
          }
        }
      }
    }
  }
}
