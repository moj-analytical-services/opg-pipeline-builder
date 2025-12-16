from opg_pipeline_builder.transform_engines.athena import AthenaTransformEngine
from opg_pipeline_builder.transform_engines.base import BaseTransformEngine
from opg_pipeline_builder.transforms import Transforms


class TestTransforms:
    database_name = "testdb"

    def get_transforms(self) -> Transforms:
        return Transforms()

    def setup_child_athena(self) -> AthenaTransformEngine:
        class ChildAthenaTransformEngine(AthenaTransformEngine):  # type: ignore
            def dummy_method(self, _: list[str], __: dict[str, str]) -> None: ...

        return ChildAthenaTransformEngine

    def setup_dummy_engine(self) -> BaseTransformEngine:
        class DummyTransformEngine(BaseTransformEngine):  # type: ignore
            def run(self, _: list[str], __: dict[str, str]) -> None:
                print("hello")

        return DummyTransformEngine

    def test_transforms_init(self) -> None:
        _ = self.get_transforms()

    def test_transforms_setattr(self) -> None:
        new_athena = self.setup_child_athena()
        transforms = self.get_transforms()
        transforms.athena = new_athena
        assert transforms.athena == new_athena

        dummy_engine = self.setup_dummy_engine()
        transforms.dummy = dummy_engine
        assert transforms.dummy == dummy_engine
