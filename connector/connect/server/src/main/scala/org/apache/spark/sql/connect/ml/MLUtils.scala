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

import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.connect.proto.{Expression, MlParams}
import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.util.Utils

object MLUtils {
  def setInstanceParams(instance: Params, params: MlParams): Unit = {
    params.getParamsMap.asScala.foreach { case (name, valueProto) =>
      val p = instance.getParam(name)
      val value = parse(p.paramValueClassTag.runtimeClass, valueProto)
      instance.set(p, value)
    }
  }

  def parse(paramType: Class[_], paramValueProto: Expression.Literal): Any = {
    val value = LiteralValueProtoConverter.toCatalystValue(paramValueProto)
    convertParamValue(paramType, value)
  }

  private def convertParamValue(paramType: Class[_], value: Any): Any = {
    // Some cases the param type might be mismatched with the value type.
    // Because in python side we only have int / float type for numeric params.
    // e.g.:
    // param type is Int but client sends a Long type.
    // param type is Long but client sends a Int type.
    // param type is Float but client sends a Double type.
    // param type is Array[Int] but client sends a Array[Long] type.
    // param type is Array[Float] but client sends a Array[Double] type.
    // param type is Array[Array[Int]] but client sends a Array[Array[Long]] type.
    // param type is Array[Array[Float]] but client sends a Array[Array[Double]] type.
    if (paramType == classOf[Byte]) {
      value.asInstanceOf[java.lang.Number].byteValue()
    } else if (paramType == classOf[Short]) {
      value.asInstanceOf[java.lang.Number].shortValue()
    } else if (paramType == classOf[Int]) {
      value.asInstanceOf[java.lang.Number].intValue()
    } else if (paramType == classOf[Long]) {
      value.asInstanceOf[java.lang.Number].longValue()
    } else if (paramType == classOf[Float]) {
      value.asInstanceOf[java.lang.Number].floatValue()
    } else if (paramType == classOf[Double]) {
      value.asInstanceOf[java.lang.Number].doubleValue()
    } else if (paramType.isArray) {
      val compType = paramType.getComponentType
      value.asInstanceOf[Array[_]].map { e =>
        convertParamValue(compType, e)
      }
    } else {
      value
    }
  }

  def parseRelationProto(
      relation: proto.Relation,
      sessionHolder: SessionHolder): DataFrame = {
    val planner = new SparkConnectPlanner(sessionHolder)
    val plan = planner.transformRelation(relation)
    Dataset.ofRows(sessionHolder.session, plan)
  }

  def getInstance[T](name: String)(implicit m: Manifest[T]): T = {
    val clazz = Utils.classForName(name)
    clazz.getConstructor().newInstance().asInstanceOf[T]
  }

  /**
   * Get the Estimator instance according to the fit command
   *
   * @param fit command
   * @return an Estimator
   */
  def getEstimator(fit: proto.MlCommand.Fit): Estimator[_] = {
    // TODO support plugin
    // Get the estimator according to the fit command
    val name = fit.getEstimator.getOperator.getName.replace("pyspark", "org.apache.spark")
    // Use reflection to create the estimator
    val estimator: Estimator[_] = getInstance(name)

    // Set parameters for the estimator
    val params = fit.getEstimator.getParams
    MLUtils.setInstanceParams(estimator, params)
    estimator
  }

  /**
   * Get the transformer instance according to the transform proto
   *
   * @param transformProto transform proto
   * @return a Transformer
   */
  def getTransformer(transformProto: proto.MlRelation.Transform): Transformer = {
    // Get the transformer name
    val name = transformProto.getTransformer.getName
    val transformerName = name.replace("pyspark", "org.apache.spark")
    val transformer: Transformer = getInstance(transformerName)
    val params = transformProto.getParams
    MLUtils.setInstanceParams(transformer, params)
    transformer
  }
}
