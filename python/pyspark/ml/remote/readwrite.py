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

from typing import cast, Type, TYPE_CHECKING

import pyspark.sql.connect.proto as pb2
from pyspark.ml.remote.serialize import serialize_ml_params, deserialize, deserialize_param
from pyspark.ml.util import MLWriter, MLReader, RL
from pyspark.ml.wrapper import JavaWrapper

if TYPE_CHECKING:
    from pyspark.ml.util import JavaMLReadable, JavaMLWritable
    from pyspark.core.context import SparkContext


class RemoteMLWriter(MLWriter):
    def __init__(self, instance: "JavaMLWritable") -> None:
        super().__init__()
        self._instance = instance

    @property
    def sc(self) -> "SparkContext":
        raise RuntimeError("Accessing SparkContext is not supported on Connect")

    def save(self, path: str) -> None:
        from pyspark.ml.wrapper import JavaModel

        if isinstance(self._instance, JavaModel):
            from pyspark.sql.connect.session import SparkSession

            session = SparkSession.getActiveSession()
            assert session is not None
            instance = cast("JavaModel", self._instance)
            params = serialize_ml_params(instance, session.client)

            assert isinstance(instance._java_obj, str)
            command = pb2.Command()
            command.ml_command.write.CopyFrom(
                pb2.MlCommand.Writer(
                    obj_ref=pb2.ObjectRef(id=instance._java_obj),
                    params=params,
                    path=path,
                    should_overwrite=self.shouldOverwrite,
                    options=self.optionMap,
                )
            )
            session.client.execute_command(command)


class RemoteMLReader(MLReader[RL]):
    def __init__(self, clazz: Type["JavaMLReadable[RL]"]) -> None:
        super().__init__()
        self._clazz = clazz

    def load(self, path: str) -> RL:
        from pyspark.sql.connect.session import SparkSession

        session = SparkSession.getActiveSession()
        assert session is not None
        # to get the java corresponding qualified class name
        java_qualified_class_name = (
            self._clazz.__module__.replace("pyspark", "org.apache.spark")
            + "."
            + self._clazz.__name__
        )
        command = pb2.Command()
        command.ml_command.read.CopyFrom(
            pb2.MlCommand.Reader(clazz=java_qualified_class_name, path=path)
        )
        (_, properties, _) = session.client.execute_command(command)
        model_info = deserialize(properties)
        session.client.add_ml_cache(model_info.obj_ref.id)

        # Get the python type
        def _get_class() -> Type[RL]:
            parts = (self._clazz.__module__ + "." + self._clazz.__name__).split(".")
            module = ".".join(parts[:-1])
            m = __import__(module, fromlist=[parts[-1]])
            return getattr(m, parts[-1])

        py_type = _get_class()
        # It must be JavaWrapper, since we're passing the string to the _java_obj
        if issubclass(py_type, JavaWrapper):
            instance = py_type(model_info.obj_ref.id)
            instance._resetUid(model_info.uid)
            params = {k: deserialize_param(v) for k, v in model_info.params.params.items()}
            instance._set(**params)
            return instance
        else:
            raise RuntimeError(f"Unsupported class {self._clazz}")
