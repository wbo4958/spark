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

import com.google.common.collect.Lists

import java.io.{DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import net.razorvine.pickle.Pickler
import org.apache.spark.api.python.{PythonFunction, PythonRDD, PythonUtils, SimplePythonFunction}
import py4j.GatewayServer
import py4j.GatewayServer.GatewayServerBuilder

object RapidsHelper {
  val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    sys.env.getOrElse("PYTHONPATH", ""))

  //  private lazy val (gw: py4j.Gateway, authToken: String) = {
  //    val gatewayserver = new RapidsGatewayServer()
  //    gatewayserver.start()
  //    (gatewayserver.getGateway, gatewayserver.getAuthToken)
  //  }

  val AUTH_TOKEN = "ABCDEFGAAA"

  def startServer(): GatewayServer = {
    val gatewayserver = new GatewayServerBuilder().authToken(AUTH_TOKEN).build()
    gatewayserver.start()
    gatewayserver
  }

  private val server = startServer()
  private val gw = server.getGateway
  private val authToken = AUTH_TOKEN

  def getAuthToken: String = authToken

  def getPythonKey(obj: Object): String = {
    gw.putNewObject(obj)
  }
}


class RapidsMLFunction extends SimplePythonFunction(
  command = Array[Byte](),
  envVars = Map("PYTHONPATH" ->
    "/home/bobwang/work.d/spark/spark-master/python:/home/bobwang/work.d/spark-rapids-ml/python/src",
    "PYSPARK_PYTHON" -> "/home/bobwang/anaconda3/envs/rapids-24.10/bin/python",
    "PYSPARK_DRIVER_PYTHON" -> "/home/bobwang/anaconda3/envs/rapids-24.10/bin/python").asJava,
  pythonIncludes = ArrayBuffer("").asJava,
  pythonExec = "/home/bobwang/anaconda3/envs/rapids-24.10/bin/python",
  pythonVer = "3.10",
  broadcastVars = Lists.newArrayList(),
  accumulator = null)

case class EstimatorFit(name: String, dataset: String, sc: String)

class PythonPlannerRunnerRapids(fit: EstimatorFit,
                                func: PythonFunction) extends PythonPlannerRunner[Int](func) {

  override protected val workerModule: String = "pyspark.sql.worker.rapids_ml_plugin"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // scalastyle:off println
    println("in writeToPython")
    // scalastyle:on println
    PythonRDD.writeUTF(RapidsHelper.getAuthToken, dataOut)
    PythonRDD.writeUTF(fit.name, dataOut)
    PythonRDD.writeUTF(fit.dataset, dataOut)
    PythonRDD.writeUTF(fit.sc, dataOut)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): Int = {
    //    val v = dataIn.readInt()
    // scalastyle:off println
    println(s"receiveFromPython")
    // scalastyle:on println
    //    v
    1
  }
}
