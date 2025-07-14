from __future__ import annotations

import os
import subprocess
import sys
from time import sleep

from prefect import flow, get_run_logger, task
from tiled.client import from_uri

from data_validation import general_data_validation


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(log_prints=True)
def end_of_run_workflow(stop_doc):
    logger = get_run_logger()
    print(f"TILED_API_KEY: {os.environ['TILED_API_KEY']} TILED_SITE_PROFILES: {os.environ['TILED_SITE_PROFILES']}")
    result = subprocess.run(f"ls $TILED_SITE_PROFILES")
    print(result.stdout)
    tiled_client = from_profile("nsls2")
    logger.info("testing, adding something new to the end_of_run_workflow")
    print("duplicate - testing, adding something new to the end_of_run_workflow")
    logger.info(f"stop doc: {stop_doc}")
    uid = stop_doc["run_start"]
    logger.info(f"tiled info: {tiled_client['fxi']['raw'][uid]}")
    return
    general_data_validation(uid)
    # export(uid)
    log_completion()


if __name__ == "__main__":
    print("end of run workflow")  # noqa: T201
    args = sys.argv
    print(f"{len(args)}, {args}")  # noqa: T201
    end_of_run_workflow({"stop_doc": args[1]})
    sleep(100)
    print("after sleep")  # noqa: T201
