from typing import Dict, List


class TestTransforms:
    database_name = "testdb"

    def get_transforms(self):
        from opg_pipeline_builder.transforms import Transforms

        return Transforms()

    def setup_child_athena(self):
        from opg_pipeline_builder.transform_engines.athena import \
            AthenaTransformEngine

        class ChildAthenaTransformEngine(AthenaTransformEngine):
            def dummy_method(self, tables: List[str], stages: Dict[str, str]): ...

        return ChildAthenaTransformEngine

    def setup_dummy_engine(self):
        from opg_pipeline_builder.transform_engines.base import \
            BaseTransformEngine

        class DummyTransformEngine(BaseTransformEngine):
            def run(self, tables: List[str], stages: Dict[str, str]):
                print("hello")

        return DummyTransformEngine

    def test_transforms_init(self):
        _ = self.get_transforms()

    def test_transforms_setattr(self):
        new_athena = self.setup_child_athena()
        transforms = self.get_transforms()
        transforms.athena = new_athena
        assert transforms.athena == new_athena

        dummy_engine = self.setup_dummy_engine()
        transforms.dummy = dummy_engine
        assert transforms.dummy == dummy_engine
