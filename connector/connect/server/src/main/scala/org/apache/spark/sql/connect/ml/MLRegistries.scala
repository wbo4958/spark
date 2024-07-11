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

package org.apache.spark.sql.connect.ml

import scala.reflect.ClassTag

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification.LogisticRegression

final case class InvalidStageInput(
    private val message: String = "",
    private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object MLRegistries {

  type StageBuilder = () => Estimator[_]

  /**
   * Helper method that constructs the stage from the type.
   *
   * @param name The name of the registered Stage.
   * @tparam T The class used to construct.
   * @return
   */
  private def stage[T <: Estimator[_] : ClassTag](name: String): (String, StageBuilder) = {
    val newBuilder = () => {
      val runtimeClass = scala.reflect.classTag[T].runtimeClass
      val ctorOpt = runtimeClass.getConstructors.find(_.getParameterCount == 0)
      if (ctorOpt.isEmpty) {
        throw InvalidStageInput("Could not find empty CTOR")
      }
      try {
        ctorOpt.get.newInstance().asInstanceOf[T]
      } catch {
        case e: Exception => throw new InvalidStageInput(e.getCause.getMessage)
      }
    }
    (name, newBuilder)
  }

  lazy val stages: Map[String, StageBuilder] = Map(
    stage[LogisticRegression]("LogisticRegression")
  )
}
