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

from typing import Optional, Dict

from pyspark.resource import ExecutorResourceRequest, TaskResourceRequest

import pyspark.sql.connect.proto as pb2


class _ResourceProfile:
    """The internal _ResourceProfile is used to create the Spark ResourceProfile
    on the server side and store the generated profile id."""
    def __init__(
        self,
        exec_req: Optional[Dict[str, ExecutorResourceRequest]] = None,
        task_req: Optional[Dict[str, TaskResourceRequest]] = None,
    ):
        from pyspark.sql.connect.session import SparkSession
        session = SparkSession.getActiveSession()
        if session is None:
            raise "SparkSession should be initialized first before ResourceProfile creation."

        exec_req = exec_req or {}
        task_req = task_req or {}

        self._exec_req = {}
        self._task_req = {}

        for key, value in exec_req.items():
            self._exec_req[key] = pb2.ExecutorResourceRequest(
                resource_name=value.resourceName,
                amount=value.amount,
                discovery_script=value.discoveryScript,
                vendor=value.vendor)

        for key, value in task_req.items():
            self._task_req[key] = pb2.TaskResourceRequest(
                resource_name=value.resourceName,
                amount=value.amount)

        self._remote_profile = pb2.ResourceProfile(
            executor_resources=self._exec_req,
            task_resources=self._task_req)

        self._id = session.client.build_resource_profile(self._remote_profile)

    @property
    def id(self) -> int:
        return self._id