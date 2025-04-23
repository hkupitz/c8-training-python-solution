import asyncio
import logging
from pyzeebe import ZeebeClient, ZeebeWorker, ZeebeTaskRouter, create_camunda_cloud_channel, Job

# Setup logging
# logging.basicConfig(level=logging.Debug)

def get_customer_credit(customer_id: str):
    return customer_id[-2:]

def deduct_credit(customer_id: str, amount: float):
    open_amount = amount - float(get_customer_credit(customer_id))
    return open_amount

# Define tasks
router = ZeebeTaskRouter()

@router.task("example-job")
def handle_example_task(job: Job):
    print(f"Hello world")
    return

# Create a channel, the worker and include the router with tasks
async def main():
    print("Worker starting")
    grpc_channel = create_camunda_cloud_channel(client_id="xxx",
                                                client_secret="xxx",
                                                cluster_id="xxx",
                                                region="bru-2")
    worker = ZeebeWorker(grpc_channel)
    worker.include_router(router)
    await worker.work()

asyncio.run(main())
