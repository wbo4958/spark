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

package org.apache.spark.executor

import org.apache.spark.internal.config.DRIVER_USER_CLASS_PATH_FIRST

import scala.util.Properties
import org.apache.spark.{JobArtifactSet, JobArtifactState, LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

import java.io.{File, PrintWriter}
import java.net.URL

class ClassLoaderIsolationSuite extends SparkFunSuite with LocalSparkContext  {

  private val scalaVersion = Properties.versionNumberString
    .split("\\.")
    .take(2)
    .mkString(".")

  val jar1 = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar").toString

  // package com.example
  // object Hello { def test(): Int = 2 }
  // case class Hello(x: Int, y: Int)
  val jar2 = Thread.currentThread().getContextClassLoader
    .getResource(s"TestHelloV2_$scalaVersion.jar").toString

  // package com.example
  // object Hello { def test(): Int = 3 }
  // case class Hello(x: String)
  val jar3 = Thread.currentThread().getContextClassLoader
    .getResource(s"TestHelloV3_$scalaVersion.jar").toString

  test("Executor classloader isolation with JobArtifactSet") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    sc.addJar(jar1)
    sc.addJar(jar2)
    sc.addJar(jar3)

    // TestHelloV2's test method returns '2'
    val artifactSetWithHelloV2 = new JobArtifactSet(
      Some(JobArtifactState(uuid = "hello2", replClassDirUri = None)),
      jars = Map(jar2 -> 1L),
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithHelloV2.state.get) {
      sc.addJar(jar2)
      sc.parallelize(1 to 1).foreach { i =>
        val cls = Utils.classForName("com.example.Hello$")
        val module = cls.getField("MODULE$").get(null)
        val result = cls.getMethod("test").invoke(module).asInstanceOf[Int]
        if (result != 2) {
          throw new RuntimeException("Unexpected result: " + result)
        }
      }
    }

    // TestHelloV3's test method returns '3'
    val artifactSetWithHelloV3 = new JobArtifactSet(
      Some(JobArtifactState(uuid = "hello3", replClassDirUri = None)),
      jars = Map(jar3 -> 1L),
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithHelloV3.state.get) {
      sc.addJar(jar3)
      sc.parallelize(1 to 1).foreach { i =>
        val cls = Utils.classForName("com.example.Hello$")
        val module = cls.getField("MODULE$").get(null)
        val result = cls.getMethod("test").invoke(module).asInstanceOf[Int]
        if (result != 3) {
          throw new RuntimeException("Unexpected result: " + result)
        }
      }
    }

    // Should not be able to see any "Hello" class if they're excluded from the artifact set
    val artifactSetWithoutHello = new JobArtifactSet(
      Some(JobArtifactState(uuid = "Jar 1", replClassDirUri = None)),
      jars = Map(jar1 -> 1L),
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithoutHello.state.get) {
      sc.addJar(jar1)
      sc.parallelize(1 to 1).foreach { i =>
        try {
          Utils.classForName("com.example.Hello$")
          throw new RuntimeException("Import should fail")
        } catch {
          case _: ClassNotFoundException =>
        }
      }
    }
  }

  test("SPARK-51537 Executor isolation session classloader inherits from " +
    "default session classloader") {

    val tempDir = Utils.createTempDir()
    val tempFileName = "test.txt"
    val tempFile = new File(tempDir, tempFileName)

    // scalastyle:off println
    Utils.tryWithResource {
      new PrintWriter(tempFile)
    } { writer =>
      writer.println("SparkPluginTest")
    }
    // scalastyle:on println

    val sparkPluginCodeBody =
      """
        |@Override
        |public org.apache.spark.api.plugin.ExecutorPlugin executorPlugin() {
        |  return new TestExecutorPlugin();
        |}
        |
        |@Override
        |public org.apache.spark.api.plugin.DriverPlugin driverPlugin() { return null; }
      """.stripMargin

    val testCodeBody =
      s"""
         | public static boolean flag = false;
         |""".stripMargin

    val compiledTestCode = TestUtils.createCompiledClass(
      "TestFoo",
      tempDir,
      "",
      null,
      Seq.empty,
      Seq.empty,
      testCodeBody)

    val executorPluginCodeBody =
      s"""
         |@Override
         |public void init(
         |    org.apache.spark.api.plugin.PluginContext ctx,
         |    java.util.Map<String, String> extraConf) {
         |  TestFoo.flag = true;
         |}
      """.stripMargin

    val thisClassPath =
      sys.props("java.class.path").split(File.pathSeparator).map(p => new File(p).toURI.toURL)

    val compiledExecutorPlugin = TestUtils.createCompiledClass(
      "TestExecutorPlugin",
      tempDir,
      "",
      null,
      Seq(tempDir.toURI.toURL) ++ thisClassPath,
      Seq("org.apache.spark.api.plugin.ExecutorPlugin"),
      executorPluginCodeBody)


    val compiledSparkPlugin = TestUtils.createCompiledClass(
      "TestSparkPlugin",
      tempDir,
      "",
      null,
      Seq(tempDir.toURI.toURL) ++ thisClassPath,
      Seq("org.apache.spark.api.plugin.SparkPlugin"),
      sparkPluginCodeBody)

    val jarUrl = TestUtils.createJar(
      Seq(compiledSparkPlugin, compiledExecutorPlugin, compiledTestCode),
      new File(tempDir, "testplugin.jar"))

    def getSubmitClassLoader(sparkConf: SparkConf): MutableURLClassLoader = {
      val loader =
        if (sparkConf.get(DRIVER_USER_CLASS_PATH_FIRST)) {
          new ChildFirstURLClassLoader(new Array[URL](0),
            Thread.currentThread.getContextClassLoader)
        } else {
          new MutableURLClassLoader(new Array[URL](0),
            Thread.currentThread.getContextClassLoader)
        }
      Thread.currentThread.setContextClassLoader(loader)
      loader
    }

    val loader = getSubmitClassLoader(new SparkConf())
    loader.addURL(jarUrl)

    sc = new SparkContext(new SparkConf()
      .setAppName("test")
      .set("spark.test.home", "/home/bobwang/work.d/spark/spark-4.0")
      .setMaster("local-cluster[1, 1, 1024]")
      .set("spark.jars", jar2 + "," + jarUrl.toString())
      .set("spark.plugins", "TestSparkPlugin"))

    // TestHelloV2's test method returns '2'
    val artifactSetWithHelloV2 = new JobArtifactSet(
      Some(JobArtifactState(uuid = "hello2", replClassDirUri = None)),
      jars = Map.empty,
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithHelloV2.state.get) {
      sc.parallelize(1 to 1).foreach { i =>

//        // Test jar2
//        val cls = Utils.classForName("com.example.Hello$")
//        val module = cls.getField("MODULE$").get(null)
//        val result = cls.getMethod("test").invoke(module).asInstanceOf[Int]
//        if (result != 2) {
//          throw new RuntimeException("Unexpected result: " + result)
//        }

        // Test Plugin

        val cls1 = Utils.classForName("TestFoo", false, true)
        val z = cls1.getField("flag").getBoolean(null)
        throw new RuntimeException("got " + z)
      }
    }
  }

  test("asdfadfasdfasdfadfa") {

  }
}
