from prefect import task, flow, get_run_logger
from data_validation import data_validation
from  prefect2_test_flow import hello_world
# from long_flow import long_flow


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    hello_world()
    data_validation(uid, return_state=True)
    # long_flow(iterations=100, sleep_length=10)
    log_completion()


if __name__ == "__main__":
    args = sys.argv
    print("end of run workflow")  # noqa: T201
    print(f"{len(args)}, {args}")  # noqa: T201
    end_of_run_workflow({"stop_doc": args[1]})
    sleep(100)
    print("after sleep")  # noqa: T201
