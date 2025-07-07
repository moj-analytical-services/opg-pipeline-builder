import argparse
import os
import sys
from copy import deepcopy

from opg_pipeline_builder import __version__ as v
from opg_pipeline_builder.pipeline import Pipeline
from opg_pipeline_builder.pipeline_builder import PipelineBuilder
from opg_pipeline_builder.utils.constants import etl_steps


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def run_pipeline_step(pipeline: Pipeline, step: str) -> None:
    etl_step_fn = getattr(pipeline, step)
    print(
        f"{bcolors.BOLD}\tRunning ETL step:{bcolors.ENDC}\n"
        f"{bcolors.OKBLUE}\t\t{etl_step_fn}{bcolors.ENDC}"
    )
    _ = etl_step_fn()
    print(f"{bcolors.BOLD}\tFinished running ETL step{bcolors.ENDC}")


def run_full_pipeline(pipeline: Pipeline) -> None:
    for etl_step in etl_steps:
        run_pipeline_step(pipeline, etl_step)


# create a keyvalue class
class keyvalue(argparse.Action):
    # Constructor calling
    def __call__(
        self,
        parser,
        namespace,
        values,
        option_string=None,
    ) -> None:
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split("=")
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value


def main():
    HERE = os.getcwd()
    sys.path.insert(0, HERE)

    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=str, help="database name")
    parser.add_argument(
        "production_environment",
        type=str,
        help="Production environment for the pipeline (e.g. dev, prod)",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=v),
    )
    parser.add_argument(
        "-t",
        "--tables",
        nargs="*",
        type=str,
        help="List of database tables to run pipeline on",
    )
    parser.add_argument("-s", "--step", type=str, help="ETL step to run")
    parser.add_argument(
        "-e", "--env", nargs="*", action=keyvalue, help="Environment variables to set"
    )
    parser.add_argument(
        "-dp", "--disable-prompts", type=bool, help="Option to disable user prompts"
    )
    print(os.getcwd())
    args = parser.parse_args()
    print(
        f"{bcolors.BOLD}\tParsed arguments:{bcolors.ENDC}\n"
        f"\t\t{bcolors.OKCYAN}{args}{bcolors.ENDC}"
    )

    env_vars = deepcopy(args.env) if args.env is not None else {}

    print(
        f"{bcolors.BOLD}\tSetting SOURCE_DB_ENV:{bcolors.ENDC}\n"
        f"\t\t{bcolors.OKCYAN}{args.database}{bcolors.ENDC}"
    )
    env_vars["SOURCE_DB_ENV"] = args.database

    print(
        f"{bcolors.BOLD}\tSetting DEFAULT_DB_ENV:{bcolors.ENDC}\n"
        f"\t\t{bcolors.OKCYAN}{args.production_environment}{bcolors.ENDC}"
    )
    env_vars["DEFAULT_DB_ENV"] = args.production_environment

    if args.tables is not None:
        print(
            f"{bcolors.BOLD}\tSetting SOURCE_TBLS_ENV:{bcolors.ENDC}\n"
            f"\t\t{bcolors.OKCYAN}{args.tables}{bcolors.ENDC}"
        )
        env_vars["SOURCE_TBLS_ENV"] = ";".join(args.tables)
    elif "SOURCE_TBLS_ENV" in os.environ:
        del os.environ["SOURCE_TBLS_ENV"]

    if args.step is not None:
        f"Setting ETL_STAGE_ENV: {args.step}"
        env_vars["ETL_STAGE_ENV"] = args.step
    elif "ETL_STAGE_ENV" in os.environ:
        del os.environ["ETL_STAGE_ENV"]

    os.environ.update(env_vars)

    pipeline = PipelineBuilder.build_pipeline_from_config(args.database)

    if args.step is None and not args.disable_prompts:
        prompt = input(
            f"{bcolors.WARNING}\tYou haven't set an ETL step. This means the whole"
            + " pipeline will be run sequentially.\n\tAre you sure you want to do"
            + " this? (Type 'Yes' to continue or anything else to abort.):\n\t\t"
            + f"{bcolors.ENDC}"
        )
        if prompt == "Yes":
            print(
                f"{bcolors.BOLD}\tRunning full pipeline for "
                f"{args.database}{bcolors.ENDC}"
            )
            run_full_pipeline(pipeline)
    elif args.step is None:
        print(
            f"{bcolors.BOLD}\tRunning full pipeline for "
            f"{args.database}{bcolors.ENDC}"
        )
        run_full_pipeline(pipeline)
    else:
        print(
            f"{bcolors.BOLD}\tRunning pipeline step {args.step} for "
            f"{args.database}{bcolors.ENDC}"
        )
        run_pipeline_step(pipeline, args.step)

    print(f"{bcolors.BOLD}\tFinished running pipeline! \U0001f389")


def entrypoint(database: str, step: str) -> None:
    pipeline = PipelineBuilder.build_pipeline_from_config(database)

    if step == "all":
        run_full_pipeline(pipeline)
    else:
        run_pipeline_step(pipeline, step)


if __name__ == "__main__":
    main()
