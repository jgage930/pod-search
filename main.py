from prefect import flow


@flow(log_prints=True)
def hello_flow(name: str = "world", goodbye: bool = False):
    print(f"Hello {name} from prefect!")

    if goodbye:
        print(f"bye {name}")


if __name__ == "__main__":
    # creates a deployment and starts a long-running
    # process that listens for scheduled work
    hello_flow.serve(
        name="my-first-deployment",
        tags=["onboarding"],
        parameters={"name": "Felcia", "goodbye": True},
        interval=60
    )