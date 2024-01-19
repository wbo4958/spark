from typing import Optional, Dict

from pyspark.resource import ExecutorResourceRequest, TaskResourceRequest
from pyspark.sql.connect.session import SparkSession

import pyspark.sql.connect.proto as pb2


class ResourceProfile:
    def __init__(
            self,
            _exec_req: Optional[Dict[str, ExecutorResourceRequest]] = None,
            _task_req: Optional[Dict[str, TaskResourceRequest]] = None,
    ):
        session = SparkSession.getActiveSession()
        if session is None:
            raise "SparkSession should be initialized first."

        _exec_req = _exec_req or {}
        _task_req = _task_req or {}

        self._exec_req = {}
        self._task_req = {}

        for key, value in _exec_req.items():
            self._exec_req[key] = pb2.ExecutorResourceRequest(
                resource_name=value.resourceName,
                amount=value.amount,
                discovery_script=value.discoveryScript,
                vendor=value.vendor)

        for key, value in _task_req.items():
            self._task_req[key] = pb2.TaskResourceRequest(
                resource_name=value.resourceName,
                amount=value.amount)

        self._remote_profile = pb2.ResourceProfile(
            executor_resources=self._exec_req,
            task_resources=self._task_req)

        self._id = session.client.build_resource_profile(self._remote_profile)
        print("-------------- finished build resource profile -------------")

    @property
    def id(self) -> int:
        return self._id
