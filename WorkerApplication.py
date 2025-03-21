import asyncio
import logging
from pyzeebe import ZeebeClient, ZeebeWorker, ZeebeTaskRouter, create_camunda_cloud_channel, Job, JobController

# Setup logging
# logging.basicConfig(level=logging.Debug)

class InvalidExpiryDateException(Exception):
    pass

def get_customer_credit(customer_id: str):
    return customer_id[-2:]

def deduct_credit(customer_id: str, amount: float):
    open_amount = amount - float(get_customer_credit(customer_id))
    return open_amount

def check_expiry_date(expiry_date):
    return len(expiry_date) == 5

def charge_credit_card(card_number: str, cvc: str, expiry_date: str):
    if check_expiry_date(expiry_date):
        print("Charging credit card with number " + card_number + ", cvc " + cvc + ", expiry date " + expiry_date)
    else:
        raise InvalidExpiryDateException("Invalid expiry date: " + expiry_date)
    return

async def credit_card_charging_exception_handler(exception: Exception, job: Job, job_controller: JobController) -> None:
    print(exception)

    if isinstance(exception, InvalidExpiryDateException):
        await job_controller.set_error_status(str(exception), "creditCardChargeError")
    else:
        await job_controller.set_failure_status(str(exception))
    return

# Define tasks
router = ZeebeTaskRouter()

@router.task("credit-deduction")
def handle_credit_deduction(job: Job, customerId: str, orderTotal: float):
    print(f"Handling job: {job.type}")
    open_amount = deduct_credit(customerId, orderTotal)
    customer_credit = get_customer_credit(customerId)
    return {'openAmount': open_amount, 'customerCredit': customer_credit}

@router.task("credit-card-charging", credit_card_charging_exception_handler)
def handle_credit_card_charging(job: Job, cardNumber: str, cvc: int, expiryDate: str):
    print(f"Handling job: {job.type}")
    charge_credit_card(cardNumber, cvc, expiryDate)
    return

@router.task("payment-invocation")
async def handle_payment_invocation(job: Job):
    print("Handling job: " + job.type)
    orderId = job.variables.get("orderId")
    await zeebe_client.publish_message("paymentRequestMessage", orderId, dict(job.variables))
    return

@router.task("payment-completion")
async def handle_payment_completion(job: Job):
    print("Handling job: " + job.type)
    orderId: str = job.variables.get("orderId")
    await zeebe_client.publish_message("paymentCompletedMessage", orderId)
    return

# Create a channel, the worker and include the router with tasks
async def main():
    global zeebe_client
    grpc_channel = create_camunda_cloud_channel(client_id="xxx",
                                            client_secret="xxx",
                                            cluster_id="xxx",
                                            region="bru-2") 
    zeebe_client = ZeebeClient(grpc_channel)
    worker = ZeebeWorker(grpc_channel)
    worker.include_router(router)
    await worker.work()

asyncio.run(main())
