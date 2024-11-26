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

@router.task("credit-deduction")
def handle_credit_deduction(job: Job, customerId: str, orderTotal: float):
    print(f"Handling job: {job.type}")
    open_amount = deduct_credit(customerId, orderTotal)
    customer_credit = get_customer_credit(customerId)
    return {'openAmount': open_amount, 'customerCredit': customer_credit}

@router.task("credit-card-charging")
def handle_credit_card_charging(job: Job, cardNumber: str, cvc: int, expiryDate: str):
    print(f"Handling job: {job.type}")
    print("Charging credit card with number " + cardNumber + ", cvc " + cvc + ", expiry date " + expiryDate)
    return

# Create a channel, the worker and include the router with tassks
async def main():
    grpc_channel = create_camunda_cloud_channel(client_id="xxx",
                                                client_secret="xxx",
                                                cluster_id="xxx",
                                                region="bru-2")
    worker = ZeebeWorker(grpc_channel)
    worker.include_router(router)
    await worker.work()

asyncio.run(main())