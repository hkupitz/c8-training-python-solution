import asyncio

from pyzeebe import ZeebeClient, ZeebeWorker, create_camunda_cloud_channel, Job

grpc_channel = create_camunda_cloud_channel(client_id="",
                                            client_secret="",
                                            cluster_id="",
                                            region="bru-2")
zeebe_client = ZeebeClient(grpc_channel)
worker = ZeebeWorker(grpc_channel)

@worker.task("credit-deduction")
def deduct_credit(job: Job):
    print("Handling job: " + job.type)
    return

@worker.task("credit-card-charging")
def deduct_credit(job: Job):
    print("Handling job: " + job.type)
    return

loop = asyncio.get_event_loop()
loop.run_until_complete(worker.work())