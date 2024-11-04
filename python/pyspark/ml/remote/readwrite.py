from typing import cast, Type

import pyspark.sql.connect.proto as pb2
from pyspark.ml.remote.serialize import serialize_ml_params
from pyspark.ml.util import MLWriter, MLReader, RL
from pyspark.sql import SparkSession


class RemoteMLWriter(MLWriter):
    def __init__(self, instance: "JavaMLWritable") -> None:
        super().__init__()
        self._instance = instance

    @property
    def sc(self, ) -> "SparkContext":
        raise RuntimeError("Accessing SparkContext is not supported on Connect")

    def save(self, path: str) -> None:
        from pyspark.ml import Model
        if isinstance(self._instance, Model):
            instance = cast("Model", self._instance)
            id = instance._java_obj
            session = SparkSession.getActiveSession()
            params = serialize_ml_params(instance, session)

            writer = pb2.MlCommand.Writer(model_ref=pb2.ModelRef(id=id),
                                          params=params,
                                          path=path,
                                          should_overwrite=self.shouldOverwrite,
                                          options=self.optionMap)
            req = session.client._execute_plan_request_with_metadata()
            req.plan.ml_command.write.CopyFrom(writer)
            session.client.execute_ml(req)


class RemoteMLReader(MLReader):
    def __init__(self, clazz: Type["JavaMLReadable[RL]"]) -> None:
        super().__init__()
        self._clazz = clazz

    def load(self, path: str) -> RL:
        java_package = self._clazz.__module__.replace("pyspark", "org.apache.spark")
        session = SparkSession.getActiveSession()
        reader = pb2.MlCommand.Reader(clazz=java_package,
                                      path=path)
        req = session.client._execute_plan_request_with_metadata()
        req.plan.ml_command.write.CopyFrom(reader)
        session.client.execute_ml(req)
