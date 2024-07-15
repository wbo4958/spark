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
import functools
import os
from typing import Any, cast, TypeVar, Callable

import pyspark.sql.connect.proto as pb2
from pyspark.ml.remote.serialize import serialize_ml_params, serialize, deserialize
from pyspark.sql import is_remote, SparkSession
from pyspark.sql.connect.dataframe import DataFrame as RemoteDataFrame


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def try_remote_intermediate_result(f):
    """Mark the function/property that returns the intermediate result of the remote call.
    Eg, model.summary"""

    @functools.wraps(f)
    def wrapped(self) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            id = cast("JavaWrapper", self)._java_obj
            return f"{id}.{f.__name__}"
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_attribute_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a Relation.
    Eg, model.summary.roc"""

    @functools.wraps(f)
    def wrapped(self, *args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            id = cast("JavaWrapper", self)._java_obj
            name = f.__name__
            session = SparkSession.getActiveSession()

            # The attribute returns a dataframe, we need to wrap it
            # in the _ModelAttributeRelationPlan
            from pyspark.ml.remote.proto import _ModelAttributeRelationPlan
            plan = _ModelAttributeRelationPlan(id, name)
            return RemoteDataFrame(plan, session)
        else:
            return f(self, *args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_fit(f: FuncT) -> FuncT:
    """Mark the function that fits a model."""

    @functools.wraps(f)
    def wrapped(self, dataset: RemoteDataFrame) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            instance = cast("JavaEstimator", self)

            def get_estimator_name(obj: Any):
                module = obj.__class__.__module__
                if module is None or module == str.__class__.__module__:
                    return obj.__class__.__name__
                else:
                    return module + '.' + obj.__class__.__name__

            estimator_name = get_estimator_name(instance)

            client = dataset.sparkSession.client
            input = dataset._plan.plan(client)
            estimator = pb2.MlStage(
                name=estimator_name,
                params=serialize_ml_params(instance, client),
                uid=instance.uid,
                type=pb2.MlStage.ESTIMATOR,
            )
            fit_cmd = pb2.MlCommand.Fit(
                estimator=estimator,
                dataset=input,
            )
            req = client._execute_plan_request_with_metadata()
            req.plan.ml_command.fit.CopyFrom(fit_cmd)
            model_id = deserialize(client.execute_ml(req))
            return model_id
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_transform_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a model transform relation."""

    @functools.wraps(f)
    def wrapped(self, dataset: RemoteDataFrame) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            id = cast("JavaWrapper", self)._java_obj
            session = SparkSession.getActiveSession()

            params = serialize_ml_params(self, session)
            from pyspark.ml.remote.proto import _ModelTransformRelationPlan
            plan = _ModelTransformRelationPlan(dataset._plan, id, params)
            return RemoteDataFrame(plan, session)
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_call(f: FuncT) -> FuncT:
    """Mark the function/property for remote call.
    Eg, model.coefficients"""

    @functools.wraps(f)
    def wrapped(self, name: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            """Launch a remote call if possible"""
            id = cast("JavaWrapper", self)._java_obj

            session = SparkSession.getActiveSession()
            get_attribute = pb2.FetchModelAttr(
                model_ref=pb2.ModelRef(id=id),
                method=name,
                args=serialize(session.client, *args)
            )
            req = session.client._execute_plan_request_with_metadata()
            req.plan.ml_command.fetch_model_attr.CopyFrom(get_attribute)

            return deserialize(session.client.execute_ml(req))
        else:
            return f(self, name, *args)

    return cast(FuncT, wrapped)


def try_remote_del(f: FuncT) -> FuncT:
    """Mark the function/property to del a model."""

    @functools.wraps(f)
    def wrapped(self) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            # Delete the model if possible
            id = cast("JavaWrapper", self)._java_obj
            if id is not None and "." not in id:
                try:
                    session = SparkSession.getActiveSession()
                    if session is not None:
                        client = session.client
                        req = client._execute_plan_request_with_metadata()
                        req.plan.ml_command.delete_model.model_ref.CopyFrom(
                            pb2.ModelRef(id=id))
                        client.execute_ml(req)
                        return
                except ImportError as e:
                    # SparkSession's down.
                    return
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_return_none(f: FuncT) -> FuncT:
    """Mark the function/property than returns none."""

    @functools.wraps(f)
    def wrapped(java_class: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return None
        else:
            return f(java_class, *args)

    return cast(FuncT, wrapped)
