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

package org.apache.spark.sql.execution.python

import net.razorvine.pickle.Pickler
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.python.{PythonFunction, PythonRDD, SimplePythonFunction}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import py4j.GatewayServer.GatewayServerBuilder

import java.io.{DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._


object FooUtil {
  val AUTH_TOKEN = "ABCDExxxxxF"

  val RAPIDS_PYTHON_FUNC = {
    val defaultPythonExec: String = "/home/bobwang/anaconda3/envs/pyspark-dev/bin/python"
    val pythonVer: String = "3.12"

    new SimplePythonFunction(
      command = Array[Byte](),
      envVars = Map(
        "PYSPARK_PYTHON" -> defaultPythonExec,
        "PYSPARK_DRIVER_PYTHON" -> defaultPythonExec,
        "PYTHONPATH" -> "/home/bobwang/work.d/spark/spark-master/python"
      ).asJava,
      pythonIncludes = ArrayBuffer("").asJava,
      pythonExec = defaultPythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty.asJava,
      accumulator = null
    )
  }
}

class BobbySuite extends QueryTest with SharedSparkSession {


  class TestPyRunner(dfKey: String, jscKey: String, func: PythonFunction) extends
    PythonPlannerRunner[Unit](func) {

    override protected val workerModule: String = "pyspark.sql.worker.connect_plugin"

    override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
      // scalastyle:off println
      println("in writeToPython")

      PythonRDD.writeUTF(FooUtil.AUTH_TOKEN, dataOut)
      PythonRDD.writeUTF(jscKey, dataOut)
      PythonRDD.writeUTF(dfKey, dataOut)
      // scalastyle:on println
    }

    override protected def receiveFromPython(dataIn: DataInputStream): Unit = {
      // scalastyle:off println
      println("in receiveFromPython")
      // scalastyle:on println
    }
  }

  test("bobby") {
    val ss = spark
    import ss.implicits._

    val df = Seq(1, 2, 3).toDF()

    val gw: py4j.Gateway = {
      val server = new GatewayServerBuilder().authToken(FooUtil.AUTH_TOKEN).build()
      server.start()
      server.getGateway
    }

    val dfKey = gw.putNewObject(df)
    val jscKey = gw.putNewObject(new JavaSparkContext(df.sparkSession.sparkContext))


    new TestPyRunner(dfKey, jscKey, FooUtil.RAPIDS_PYTHON_FUNC).runInPython(useDaemon = false)

  }


}
