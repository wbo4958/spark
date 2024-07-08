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

import org.apache.commons.lang3.reflect.MethodUtils.invokeMethod

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.MlCommand.MlCommandTypeCase
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Model
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder

object MLHandler {
  def handleMlCommand(
      sessionHolder: SessionHolder,
      mlCommand: proto.MlCommand): proto.MlCommandResponse = {

    val mlCache = sessionHolder.mlCache

    mlCommand.getMlCommandTypeCase match {
      case MlCommandTypeCase.FIT =>
        val fitProto = mlCommand.getFit
        val estimatorProto = fitProto.getEstimator
        assert(estimatorProto.getType == proto.MlStage.StageType.ESTIMATOR)

        val name = fitProto.getEstimator.getName
        val params = fitProto.getEstimator.getParams
        val dataset = parseRelationProto(fitProto.getDataset, sessionHolder)
        val model = new EstimatorHandler(name, params).fit(dataset)
        val id = mlCache.register(model)

        proto.MlCommandResponse.newBuilder()
          .setModelInfo(
            proto.MlCommandResponse.ModelInfo.newBuilder()
              .setModelRef(proto.ModelRef.newBuilder().setId(id))
              .setModelUid(model.uid))
          .build()

      case MlCommandTypeCase.FETCH_MODEL_ATTR =>
        val fetchProto = mlCommand.getFetchModelAttr
        val ids = fetchProto.getModelRef.getId.split("\\.")
        assert(ids.length > 1, "Must have an attribute")
        val modelId = ids.head
        val model = mlCache.get(modelId).asInstanceOf[Any]

        // TODO try below invokeMethod
        val x = ids.slice(1, ids.length).foldLeft(model) { (obj, attribute) =>
          invokeMethod(obj, attribute)
        }
        Serializer.serialize(x, fetchProto.getModelRef.getId)

      case _ => throw new UnsupportedOperationException("MLHandler")
    }
  }

  private def parseRelationProto(
      relationProto: proto.Relation,
      sessionHolder: SessionHolder): DataFrame = {
    val relationalPlanner = new SparkConnectPlanner(sessionHolder)
    val plan = relationalPlanner.transformRelation(relationProto)
    Dataset.ofRows(sessionHolder.session, plan)
  }


  def transformMLRelation(
      mlRelationProto: proto.MlRelation,
      sessionHolder: SessionHolder): DataFrame = {

    val mlCache = sessionHolder.mlCache

    mlRelationProto.getMlRelationTypeCase match {
      case proto.MlRelation.MlRelationTypeCase.MODEL_TRANSFORM =>
        val modelTransformRelationProto = mlRelationProto.getModelTransform
        val model = mlCache.get(modelTransformRelationProto.getModelRef.getId)
        // TODO this case should be refined.
        // Create a copied model to avoid concurrently modify model params.
        val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
        MLUtils.setInstanceParams(copiedModel, modelTransformRelationProto.getParams)
        val inputDF = parseRelationProto(modelTransformRelationProto.getInput, sessionHolder)
        copiedModel.transform(inputDF)

//      case proto.MlRelation.MlRelationTypeCase.MODEL_ATTR =>
//        val modelAttrProto = mlRelationProto.getModelAttr
//        val (model, algo) =
//          sessionHolder.mlCache.modelCache.get(modelAttrProto.getModelRef.getId)
//        algo.getModelAttr(model, modelAttrProto.getName).right.get
//
//      case proto.MlRelation.MlRelationTypeCase.MODEL_SUMMARY_ATTR =>
//        val modelSummaryAttr = mlRelationProto.getModelSummaryAttr
//        val (model, algo) =
//          sessionHolder.mlCache.modelCache.get(modelSummaryAttr.getModelRef.getId)
//        // Create a copied model to avoid concurrently modify model params.
//        val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
//        MLUtils.setInstanceParams(copiedModel, modelSummaryAttr.getParams)
//
//        val datasetOpt = if (modelSummaryAttr.hasEvaluationDataset) {
//          val evalDF =
//            MLUtils.parseRelationProto(modelSummaryAttr.getEvaluationDataset, sessionHolder)
//          Some(evalDF)
//        } else {
//          None
//        }
//        algo.getModelSummaryAttr(copiedModel, modelSummaryAttr.getName, datasetOpt).right.get

      case _ =>
        throw new IllegalArgumentException("Unsupported ml relation")
    }
  }


}
