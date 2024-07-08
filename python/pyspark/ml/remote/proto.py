from pyspark.ml.connect.serialize import serialize_ml_params
from pyspark.sql.connect.plan import LogicalPlan
import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.ml_pb2 as ml_pb2
import pyspark.sql.connect.proto.ml_common_pb2 as ml_common_pb2


class ModelRef:

    def __init__(self, ref_id: str) -> None:
        self._ref_id = ref_id

    def to_proto(self):
        return ml_common_pb2.ModelRef(id=self._ref_id)

    @classmethod
    def from_proto(cls, model_ref_pb: ml_common_pb2.ModelRef):
        return ModelRef(ref_id=model_ref_pb.id)

    def __del__(self):
        # TODO support __del__
        pass
        # client = pyspark_session._active_spark_session.client
        # del_model_proto = ml_pb2.MlCommand.DeleteModel(
        #     model_ref=self.to_proto(),
        # )
        # req = client._execute_plan_request_with_metadata()
        # req.plan.ml_command.delete_model.CopyFrom(del_model_proto)
        # client._execute_ml(req)


class _ModelTransformRelationPlan(LogicalPlan):
    def __init__(self, child, model_ref, ml_params):
        super().__init__(child)
        self._model_ref = ModelRef(model_ref)
        self._ml_params = ml_params

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        plan = self._create_proto_relation()
        if self._child is not None:
            plan.ml_relation.model_transform.input.CopyFrom(self._child.plan(session))
        plan.ml_relation.model_transform.model_ref.CopyFrom(self._model_ref.to_proto())
        if self._ml_params is not None:
            plan.ml_relation.model_transform.params.CopyFrom(self._ml_params)

        return plan
