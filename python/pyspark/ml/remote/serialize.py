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
from typing import Any, List, TYPE_CHECKING

import pyspark.sql.connect.proto as pb2
from pyspark.ml.linalg import Vectors, Matrices, DenseVector, SparseVector
from pyspark.sql import DataFrame
from pyspark.sql.connect.expressions import LiteralExpression

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.ml.param import Params


def serialize(client: "SparkConnectClient", *args: Any) -> List[Any]:
    result = []
    for arg in args:
        if isinstance(arg, DenseVector):
            vec = pb2.Vector(dense=pb2.Vector.Dense(value=arg.values.tolist()))
            result.append(pb2.FetchModelAttr.Args(vector=vec))
        elif isinstance(arg, SparseVector):
            v = pb2.Vector.Sparse(size=arg.size,
                                  index=arg.indices.tolist(),
                                  value=arg.values.tolist())
            result.append(pb2.FetchModelAttr.Args(vector=v))
        elif isinstance(arg, (int, float, str, bool)):
            result.append(pb2.FetchModelAttr.Args(literal=arg))
        elif isinstance(arg, DataFrame):
            result.append(pb2.FetchModelAttr.Args(input=arg._plan.plan(client)))
        else:
            raise RuntimeError(f"Unsupported {arg}")
    return result


def deserialize(ml_command_result: pb2.MlCommandResponse) -> Any:
    if ml_command_result.HasField("literal"):
        return LiteralExpression._to_value(ml_command_result.literal)

    if ml_command_result.HasField("model_ref"):
        return ml_command_result.model_ref.id

    if ml_command_result.HasField("vector"):
        vector = ml_command_result.vector
        if vector.HasField("dense"):
            return Vectors.dense(vector.dense.value)
        raise ValueError()

    if ml_command_result.HasField("matrix"):
        matrix = ml_command_result.matrix
        if matrix.HasField("dense") and not matrix.dense.is_transposed:
            return Matrices.dense(
                matrix.dense.num_rows,
                matrix.dense.num_cols,
                matrix.dense.value,
            )
        raise ValueError()

    raise ValueError()


def serialize_ml_params(instance: "Params", client: "SparkConnectClient") -> pb2.MlParams:
    params = {
        k.name: LiteralExpression._from_value(v).to_plan(client).literal
        for k, v in instance._paramMap.items()
    }

    return pb2.MlParams(params=params)
