"""Example Prefect flow for testing rules_prefect."""

from prefect import flow


@flow(log_prints=True)
def my_flow(name: str = "world") -> None:
    print(f"Hello, {name}!")


if __name__ == "__main__":
    my_flow()
