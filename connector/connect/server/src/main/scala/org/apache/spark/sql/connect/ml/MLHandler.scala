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

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.commons.lang3.reflect.MethodUtils.invokeMethod

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.MlCommand.MlCommandTypeCase
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.ml.Serializer.deserializeMethodArguments
import org.apache.spark.sql.connect.service.SessionHolder

private class ModelAttributeHelper(val sessionHolder: SessionHolder,
                                   val objIdentifier: String,
                                   val method: Option[String],
                                   val argValues: Array[Object] = Array.empty,
                                   val argClasses: Array[Class[_]] = Array.empty) {

  val methodChain = method.map(n => s"$objIdentifier.$n").getOrElse(objIdentifier)
  private val methodChains = methodChain.split("\\.")
  private val modelId = methodChains.head

  private lazy val model = sessionHolder.mlCache.get(modelId)
  private lazy val methods = methodChains.slice(1, methodChains.length)

  def getAttribute: Any = {
    assert(methods.length >= 1)
    val lastMethod = methods.last
    if (methods.length == 1) {
      invokeMethod(model.asInstanceOf[Object], lastMethod, argValues, argClasses)
    } else {
      val prevMethods = methods.slice(0, methods.length - 1)
      val finalObj = prevMethods.foldLeft(model.asInstanceOf[Object]) { (obj, attribute) =>
        invokeMethod(obj, attribute)
      }
      invokeMethod(finalObj, lastMethod, argValues, argValues)
    }
  }

  def transform(modelRelation: proto.MlRelation.ModelTransform): DataFrame = {
    // Create a copied model to avoid concurrently modify model params.
    val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
    MLUtils.setInstanceParams(copiedModel, modelRelation.getParams)
    val inputDF = MLUtils.parseRelationProto(modelRelation.getInput, sessionHolder)
    copiedModel.transform(inputDF)
  }
}

private object ModelAttributeHelper {
  def apply(sessionHolder: SessionHolder,
            modelId: String,
            methodChain: Option[String] = None,
            args: Array[proto.FetchModelAttr.Args] = Array.empty): ModelAttributeHelper = {
    val tmp = deserializeMethodArguments(args, sessionHolder)
    val argValues = tmp.map(_._1)
    val argClasses = tmp.map(_._2)
    new ModelAttributeHelper(sessionHolder, modelId, methodChain, argValues, argClasses)
  }
}

object MLHandler {
  def handleMlCommand(
      sessionHolder: SessionHolder,
      mlCommand: proto.MlCommand): proto.MlCommandResponse = {

    val mlCache = sessionHolder.mlCache

    mlCommand.getMlCommandTypeCase match {
      case MlCommandTypeCase.FIT =>
        val fitCmd = mlCommand.getFit
        val estimatorProto = fitCmd.getEstimator
        assert(estimatorProto.getType == proto.MlStage.StageType.ESTIMATOR)

        val name = fitCmd.getEstimator.getName
        val params = fitCmd.getEstimator.getParams
        val dataset = MLUtils.parseRelationProto(fitCmd.getDataset, sessionHolder)

        val estimator = MLRegistries.stages(name)()
        MLUtils.setInstanceParams(estimator, params)

        val model = estimator.fit(dataset).asInstanceOf[Transformer]
        val id = mlCache.register(model)

        proto.MlCommandResponse.newBuilder()
          .setModelRef(proto.ModelRef.newBuilder().setId(id))
          .build()

      case MlCommandTypeCase.FETCH_MODEL_ATTR =>
        val args = mlCommand.getFetchModelAttr.getArgsList.asScala.toArray
        val helper = ModelAttributeHelper(sessionHolder,
          mlCommand.getFetchModelAttr.getModelRef.getId,
          Option(mlCommand.getFetchModelAttr.getMethod),
          args)
        Serializer.serialize(helper.getAttribute, helper.methodChain)

      case _ => throw new UnsupportedOperationException("Unsupported ML command")
    }
  }

  def transformMLRelation(
      relation: proto.MlRelation,
      sessionHolder: SessionHolder): DataFrame = {
    relation.getMlRelationTypeCase match {
      case proto.MlRelation.MlRelationTypeCase.MODEL_TRANSFORM =>
        val helper = ModelAttributeHelper(sessionHolder,
          relation.getModelTransform.getModelRef.getId, None)
        helper.transform(relation.getModelTransform)

      case proto.MlRelation.MlRelationTypeCase.MODEL_ATTR =>
        val helper = ModelAttributeHelper(sessionHolder,
          relation.getModelAttr.getModelRef.getId,
          Option(relation.getModelAttr.getMethod))
        helper.getAttribute.asInstanceOf[DataFrame]

      case _ =>
        throw new IllegalArgumentException("Unsupported ml relation")
    }
  }

}
