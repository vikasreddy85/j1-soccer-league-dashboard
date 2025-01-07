from prefect import flow, task
from prefect.docker import DockerImage

@task
def print_hello(name):
    print(f"Hello {name}!")

@flow(name="J1 League ETL", log_prints=True)
def web_scrape(name="world"):
    print_hello(name)

if __name__ == "__main__":
    web_scrape()
