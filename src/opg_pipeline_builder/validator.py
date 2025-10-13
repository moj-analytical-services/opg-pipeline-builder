import os
from copy import deepcopy
from itertools import chain
from pathlib import Path
from typing import Any

import yaml
from croniter import croniter
from data_linter import validation
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from opg_pipeline_builder.utils.constants import etl_stages, sql_path, transform_types
