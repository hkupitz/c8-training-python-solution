import asyncio

from pyzeebe import ZeebeClient, ZeebeWorker, create_camunda_cloud_channel, Job

grpc_channel = create_camunda_cloud_channel(client_id="",
                                            client_secret="",
                                            cluster_id="",
                                            region="bru-2")
zeebe_client = ZeebeClient(grpc_channel)
worker = ZeebeWorker(grpc_channel)

class InvalidExpiryDateException(Exception):
    pass

def get_customer_credit(customer_id: str):
    return customer_id[-2:]


def deduct_credit(customer_id: str, amount: float):
    open_amount = amount - float(get_customer_credit(customer_id))
    return open_amount


def check_expiry_date(expiry_date):
    return len(expiry_date) == 5

async def credit_card_charging_exception_handler(exception: Exception, job: Job) -> None:
    print(exception)

    if exception.__class__.__name__ == "InvalidExpiryDateException":
        await job.set_error_status(str(exception), "creditCardChargeError")
    else:
        await job.set_failure_status(str(exception))
    return

@worker.task("credit-deduction")
def handle_credit_deduction(job: Job, customerId: str, orderTotal: float):
    print("Handling job: " + job.type)
    open_amount = deduct_credit(customerId, orderTotal)
    customer_credit = get_customer_credit(customerId)
    return {'openAmount': open_amount, 'customerCredit': customer_credit}

@worker.task("credit-card-charging", credit_card_charging_exception_handler)
def handle_credit_card_charging(job: Job, cardNumber: str, cvc: str, expiryDate: str, openAmount: float):
    print("Handling job: " + job.type)
    charge_credit_card(cardNumber, cvc, expiryDate, openAmount)
    return

def charge_credit_card(card_number: str, cvc: str, expiry_date: str, order_amount: float):
    if check_expiry_date(expiry_date):
        print("Charging credit card with number " + card_number + ", cvc " + cvc + ", expiry date " + expiry_date)
    else:
        raise InvalidExpiryDateException("Invalid expiry date: " + expiry_date)
    return

@worker.task("payment-invocation")
async def handle_payment_invocation(job: Job):
    print("Handling job: " + job.type)
    orderId = job.variables.get("orderId")
    await zeebe_client.publish_message("paymentRequestMessage", orderId, job.variables)
    return

@worker.task("payment-completion")
async def handle_payment_completion(job: Job):
    print("Handling job: " + job.type)
    orderId: str = job.variables.get("orderId")
    await zeebe_client.publish_message("paymentCompletedMessage", orderId)
    return

asyncio.get_event_loop().run_until_complete(worker.work())
