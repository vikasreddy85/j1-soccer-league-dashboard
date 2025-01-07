from prefect import flow, task
from prefect.docker import DockerImage

@task
def print_hello(name):
    print(f"Hello {name}!")

@flow(name="J1 League ETL", log_prints=True)
def web_scrape(name="world"):
    print_hello(name)

if __name__ == "__main__":
    flow.from_source(
        "https://github.com/vikasreddy85/j1-soccer-league-dashboard.git",
        entrypoint="flows/scrape.py:web_scrape",
    ).deploy(
        name="j1-league-container",
        work_pool_name="my-docker-pool",
        build=False
    )
