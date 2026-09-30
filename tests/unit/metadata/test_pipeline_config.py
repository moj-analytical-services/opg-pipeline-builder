import pytest

from opg_pipeline_builder.models import modelling_exceptions as exc
from opg_pipeline_builder.models.pipeline_config import PipelineConfig


def create_pipeline_config(
    name: str = "name",
    description: str = "description",
    land_path: str = "s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
    archive_path: str = "s3://bucket-name/{{ env }}/{{ db }}/archive/table-name",
    curated_path: str = "s3://bucket-name/{{ env }}/{{ db }}/curated/table-name",
) -> PipelineConfig:
    """Create a PipelineConfig instance with default or provided values."""
    return PipelineConfig(
        name=name,
        description=description,
        land_path=land_path,
        archive_path=archive_path,
        curated_path=curated_path,
    )


class TestPipelineConfig:
    @pytest.mark.parametrize(
        ("name"),
        [
            ("tablename"),
            ("table_name"),
            ("table123"),
            ("t"),
            ("a_very_long_table_name_that_is_under_sixty_three_characters"),
        ],
    )
    def test_validate_name_valid(self, name: str) -> None:
        config = create_pipeline_config(name=name)
        assert config.name == name

    @pytest.mark.parametrize(
        ("name"),
        [
            (""),
            ("table name"),
            ("table*name"),
            ("123table"),
            ("_table_name"),
            ("TableName"),
            ("a" * 64),
        ],
    )
    def test_validate_name_invalid(self, name: str) -> None:
        with pytest.raises(exc.InvalidPipelineNameError):
            create_pipeline_config(name=name)

    def test_validate_s3_paths_valid(self) -> None:
        config = create_pipeline_config()
        assert config.land_path == "s3://bucket-name/{{ env }}/{{ db }}/land/table-name"
        assert (
            config.archive_path
            == "s3://bucket-name/{{ env }}/{{ db }}/archive/table-name"
        )
        assert (
            config.curated_path
            == "s3://bucket-name/{{ env }}/{{ db }}/curated/table-name"
        )

    @pytest.mark.parametrize(
        ("path_type_to_test", "filepath", "exp_error"),
        [
            (
                "land_path",
                "",
                "S3 path cannot be empty",
            ),
            (
                "archive_path",
                "bucket-name/{{ env }}/{{ db }}/archive/table-name",
                "S3 path must start with 's3://'",
            ),
            (
                "curated_path",
                "s3:/bucket-name/{{ env }}/{{ db }}/curated/table-name",
                "S3 path must start with 's3://'",
            ),
            (
                "land_path",
                "s3://bucket-name/{{ db }}/land/table-name",
                "S3 path must contain an environment variable placeholder '{{ env }}' as a subdirectory",
            ),
            (
                "archive_path",
                "s3://bucket-name/{{ env }}/archive/table-name",
                "S3 path must contain a database name variable placeholder '{{ db }}' as a subdirectory",
            ),
            (
                "land_path",
                "s3://bucket-name/{{ env }}/{{ db }}/table-name",
                "S3 path must contain the corresponding etl stage 'land' as a subdirectory",
            ),
            (
                "archive_path",
                "s3://bucket-name/{{ env }}/{{ db }}/invalid/table-name",
                "S3 path must contain the corresponding etl stage 'archive' as a subdirectory",
            ),
            (
                "curated_path",
                "s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
                "S3 path must contain the corresponding etl stage 'curated' as a subdirectory",
            ),
        ],
    )
    def test_validate_s3_paths_invalid(
        self, path_type_to_test: str, filepath: str, exp_error: str
    ) -> None:
        with pytest.raises(exc.InvalidPathError, match=exp_error):
            create_pipeline_config(**{path_type_to_test: filepath})
