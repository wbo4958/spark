#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql.connect.plan import LogicalPlan
import pyspark.sql.connect.proto as pb2


class _ModelTransformRelationPlan(LogicalPlan):
    """_ModelTransformRelationPlan represents the model transform
    """

    def __init__(self, child, model_id, ml_params):
        super().__init__(child)
        self._model_id = model_id
        self._ml_params = ml_params

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        plan = self._create_proto_relation()
        plan.ml_relation.model_transform.input.CopyFrom(self._child.plan(session))
        plan.ml_relation.model_transform.model_ref.CopyFrom(
            pb2.ModelRef(id=self._model_id))
        if self._ml_params is not None:
            plan.ml_relation.model_transform.params.CopyFrom(self._ml_params)

        return plan


class _ModelAttributeRelationPlan(LogicalPlan):
    """_ModelAttributeRelationPlan used to represent an attribute of a "model",
    which returns a Dataframe
    """

    def __init__(self, model_id: str, method: str):
        super().__init__(None)
        self._model_id = model_id
        self._method = method

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        plan = self._create_proto_relation()
        plan.ml_relation.model_attr.model_ref.CopyFrom(pb2.ml_common_pb2.ModelRef(id=self._model_id))
        plan.ml_relation.model_attr.method = self._method
        return plan